/*
DROP TABLE IF EXISTS Peers, Tasks, Checks, P2P, Verter CASCADE;
DROP TABLE IF EXISTS Recommendations, Friends, TimeTracking, TransferredPoints, XP, CASCADE;
DROP PROCEDURE IF EXISTS import_csv_data, export_csv_data;
 */
/*
-- Тестовые запросы/вызовы для проверки процедур
-- 1)
CALL delete_tables_beginning('x');
-- 2)
DO
$$
DECLARE
    function_count INTEGER;
    function_list TEXT;
BEGIN
    CALL get_scalar_functions(function_count, function_list);

    -- Выводим результат работы процедуры
    RAISE NOTICE 'Function Count: %', function_count;
    RAISE NOTICE 'Function List: %', function_list;
END;
$$;
-- 3)
DO $$
DECLARE
    destroyed_triggers_count INTEGER;
BEGIN
    CALL destroy_triggers(destroyed_triggers_count);
    RAISE NOTICE 'Number of Triggers Destroyed: %', destroyed_triggers_count;
END $$;
-- 4)
CALL search_object_types('calculate');
*/

-- 1) удаление всех таблиц начинающихся с 'Tablename'
DROP PROCEDURE IF EXISTS delete_tables_beginning (tablename TEXT);
CREATE OR REPLACE PROCEDURE delete_tables_beginning (tablename TEXT)
LANGUAGE plpgsql AS
$$
DECLARE
    table_name_var TEXT;
BEGIN
    FOR table_name_var IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_name LIKE tablename || '%' -- ищем подходящую таблицу по маске
          AND table_schema NOT LIKE 'pg_%'     -- исключаем системные
          AND table_schema <> 'information_schema'  -- исключаем системные
    LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || table_name_var || ' CASCADE;'; -- удаляем подходящую по маске таблицу и все связанные с ней
    END LOOP;
END;
$$;

/* 
CALL delete_tables_beginning('x')
*/
-- 2) поиск пользовательских функций
DROP PROCEDURE IF EXISTS get_scalar_functions(OUT function_count INTEGER, OUT function_list TEXT);
CREATE OR REPLACE PROCEDURE get_scalar_functions(OUT function_count INTEGER, OUT function_list TEXT)
LANGUAGE plpgsql
AS $$
BEGIN
    DECLARE
        function_info RECORD;
        function_list_temp TEXT := '';
        function_count_temp INTEGER := 0;
    BEGIN
        -- Получем имена и параметры пользовательских функций
        FOR function_info IN (
            SELECT p.proname || '(' || pg_catalog.pg_get_function_arguments(p.oid) || ')' AS function_info
            FROM pg_catalog.pg_proc p
            JOIN pg_catalog.pg_namespace n ON p.pronamespace = n.oid
            WHERE n.nspname NOT LIKE 'pg_%' AND n.nspname <> 'information_schema' -- Исключаем системные функции
            AND p.prokind = 'f'                                                   -- Оставляем только скалярные функции
            AND pg_catalog.pg_get_function_result(p.oid) <> 'trigger'             -- Исключаеи триггеры
            AND pg_catalog.pg_get_function_arguments(p.oid) <> ''                 -- Исключаем функции без параметров
        )
		-- добавляем через запятую найденные функции к function_list_temp и увеличиваем счетчик на 1
        LOOP
            function_list_temp := function_list_temp || function_info.function_info || ', ';
            function_count_temp := function_count_temp + 1;
        END LOOP;

        -- Удаляем последнюю запятую и пробел после последней найденной функции
        function_list_temp := rtrim(function_list_temp, ', ');

        -- Записываем найденные значения в OUT
        function_count := function_count_temp;
        function_list := function_list_temp;
    END;
END;
$$;

/*
-- Процедура не выводит значения сама. Записываем результат в переменные и выводим их
DO
$$
DECLARE
    function_count INTEGER;
    function_list TEXT;
BEGIN
    CALL get_scalar_functions(function_count, function_list);

    -- Выводим результат работы процедуры
    RAISE NOTICE 'Function Count: %', function_count;
    RAISE NOTICE 'Function List: %', function_list;
END;
$$;
*/

-- 3) Процедура удаления DML триггеров
DROP PROCEDURE IF EXISTS destroy_triggers(OUT num_triggers_destroyed INTEGER);
CREATE OR REPLACE PROCEDURE destroy_triggers(OUT num_triggers_destroyed INTEGER)
LANGUAGE plpgsql
AS $$
DECLARE
    trigger_record RECORD;
    table_name TEXT;
BEGIN
    num_triggers_destroyed := 0;

    -- Курсор для выбора trigger names
    FOR trigger_record IN (
        SELECT tgname, tgrelid::regclass AS table_oid, tgtype
        FROM pg_trigger
    )
    LOOP
        -- Получение имеи таблицы используя таблицу OID
        SELECT relname INTO table_name
        FROM pg_class
        WHERE oid = trigger_record.table_oid;

        -- Проверяем, является ли трггер SQL DML основываясь на tgtype
        IF (trigger_record.tgtype & 14) <> 0 THEN
            -- Генерируем и выполняем  DROP TRIGGER для найденных триггеров
            BEGIN
                EXECUTE 'DROP TRIGGER IF EXISTS ' || quote_ident(trigger_record.tgname) || ' ON ' || quote_ident(table_name);
                num_triggers_destroyed := num_triggers_destroyed + 1;
            EXCEPTION
                WHEN others THEN
                    -- Если выдаются ошибки - игнорируем их
                    CONTINUE;
            END;
        END IF;
    END LOOP;
END;
$$;

/*
DO $$
DECLARE
    destroyed_triggers_count INTEGER;
BEGIN
    CALL destroy_triggers(destroyed_triggers_count);
    RAISE NOTICE 'Number of Triggers Destroyed: %', destroyed_triggers_count;
END $$;
*/

-- 4) Поиск процедур и функций по шаблону
DROP PROCEDURE IF EXISTS search_object_types(p_search_string VARCHAR);
CREATE OR REPLACE PROCEDURE search_object_types(p_search_string VARCHAR)
LANGUAGE plpgsql
AS $$
DECLARE
    object_name VARCHAR;
    object_description VARCHAR;
BEGIN
    --выбираем из таблицы pg_catalog.pg_proc процедуры и функции, подходящие под заданное название
    FOR object_name, object_description IN
        SELECT p.proname, pg_catalog.obj_description(p.oid, 'pg_proc')
        FROM pg_catalog.pg_proc p
        WHERE p.proname ILIKE '%' || p_search_string || '%' -- ILIKE ищет по шаблону без учёта регистра 
        AND (p.prokind = 'p' OR p.prokind = 'f') -- из всех данных выбираем процедуры и функции по столбцу prokind
    LOOP
	    -- выводим данные в цикле
        RAISE NOTICE 'Object Name: %, Description: %', object_name, object_description;
    END LOOP;
END;
$$;

/*
CALL search_object_types('calculate');
*/