/*
-- Тестовые запросы/вызовы для проверки функций
-- 1)
SELECT * FROM count_transferred_points(); 
-- 2)
SELECT * FROM xp_gained_foreachtask();
-- 3)
SELECT * FROM vampires('2023-05-03');
SELECT * FROM vampires('2023-05-05');
-- 4)
SELECT * FROM calculate_points_change();
-- 5)
SELECT * FROM calculate_points_balance();
-- 6)
SELECT * FROM popular_task();
-- 7)
SELECT * FROM all_tasks_in_block('C');
SELECT * FROM all_tasks_in_block('DO');
SELECT * FROM all_tasks_in_block('CPP');
-- 8)
SELECT * FROM the_best_checker();
-- 9)
SELECT* FROM count_of_peers_started_bloks('C', 'CPP');
-- 10)
SELECT * FROM Birthday_Checks();
-- 11) 
SELECT * FROM one_two_notthree('DO3_s21_LinuxMonitoring_v1.0', 'DO4_s21_LinuxMonitoring_v2.0', 'DO5_s21_SimpleDocker');
SELECT * FROM one_two_notthree('C4_s21_Math', 'C5_s21_Decimal', 'DO1_s21_Linux');
SELECT * FROM one_two_notthree('C4_s21_Math', 'C3_s21_String+', 'C8_s21_3DViewer_v1.0');
-- 12)
SELECT * FROM count_of_previous_tasks();
-- 13)
SELECT * FROM find_successful_days(3);
SELECT * FROM find_successful_days(4);
SELECT * FROM find_successful_days(5);
-- 14)
SELECT * FROM Super_Brain();
-- 15)
SELECT * from find_peers_with_early_arrivals(2, '13:59:59');
-- 16)
SELECT * FROM counting_exits(1, 1);
-- 17)
SELECT * FROM get_early_entries_stats();
*/

-- 1) Написать функцию, возвращающую таблицу TransferredPoints в более человекочитаемом виде
-- Ник пира 1, ник пира 2, количество переданных пир поинтов. 
-- Количество отрицательное, если пир 2 получил от пира 1 больше поинтов.
CREATE OR REPLACE FUNCTION  count_transferred_points()
  RETURNS TABLE (
    "Peer1" TEXT,
    "Peer2" TEXT,
    "PointsAmount" INTEGER
  )
AS $$
BEGIN
  RETURN QUERY
 WITH temp AS (
	SELECT
    TP.CheckingPeer AS Peer1,
    TP.CheckedPeer AS Peer2,
    COALESCE(TP.PointsAmount, 0) - COALESCE(TP2.PointsAmount, 0) AS PointsAmount
  FROM
    TransferredPoints TP
  LEFT JOIN
    TransferredPoints TP2 ON TP.CheckingPeer = TP2.CheckedPeer AND TP.CheckedPeer = TP2.CheckingPeer
  ORDER BY 1, 2)
  
  SELECT DISTINCT ON (LEAST(temp.peer1, temp.peer2), GREATEST(temp.peer1, temp.peer2))
    CASE WHEN temp.peer1 < temp.peer2 THEN temp.peer1 ELSE temp.peer2 END AS Peer1,
    CASE WHEN temp.peer1 < temp.peer2 THEN temp.peer2 ELSE temp.peer1 END AS Peer2,
    CASE WHEN temp.peer1 < temp.peer2 THEN COALESCE(temp.PointsAmount, 0) ELSE -COALESCE(temp.PointsAmount, 0) END AS PointsAmount
  FROM temp;
  
END;
$$ LANGUAGE plpgsql;
-- Вызов функции
/*
SELECT * FROM count_transferred_points(); 
*/

-- 2) Написать функцию, которая возвращает таблицу вида: ник пользователя, название проверенного задания, кол-во полученного XP
DROP FUNCTION IF EXISTS xp_gained_foreachtask();
CREATE OR REPLACE FUNCTION xp_gained_foreachtask()
RETURNS TABLE ("Peeer" TEXT, "Task" TEXT, "XP" INTEGER) AS $$
BEGIN
    RETURN QUERY
	SELECT Peer, Task, XP.XPAmount AS XP FROM Checks 
	JOIN XP ON Checks.ID = XP."Check"
	JOIN P2P ON P2P."Check" = Checks.ID 
	LEFT JOIN Verter ON Verter."Check" = Checks.ID 
	WHERE P2P.State = 'success' AND Checks.Task > 'C6_s21_Matrix'
	OR P2P.State = 'success' AND Verter.State = 'success'
	ORDER BY 1, 2, 3;
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM xp_gained_foreachtask();
*/

-- 3) Написать функцию, определяющую пиров, которые не выходили из кампуса в течение всего дня
-- Параметры функции: день, например 12.05.2022. 
-- Функция возвращает только список пиров.
DROP FUNCTION IF EXISTS vampires(IN ddate date);
CREATE OR REPLACE FUNCTION vampires(IN ddate date)
RETURNS TABLE ("Peer" text) AS $$
BEGIN
    RETURN QUERY
	SELECT Peer FROM TimeTracking 
	WHERE TimeTracking.Date = ddate 
	GROUP BY 1 HAVING SUM(State) = 1
	ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM vampires('2023-05-03');
SELECT * FROM vampires('2023-05-05');
*/

-- 4) Посчитать изменение в количестве пир поинтов каждого пира по таблице TransferredPoints
-- Результат вывести отсортированным по изменению числа поинтов. 
-- Формат вывода: ник пира, изменение в количество пир поинтов
DROP FUNCTION IF EXISTS calculate_points_change();
CREATE OR REPLACE FUNCTION calculate_points_change()
RETURNS TABLE (nickname TEXT, pointschange BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT t1.Nickname, t2.points_received - t1.points_spent AS points_change
    FROM (
        SELECT p.Nickname, COALESCE(SUM(tp1.PointsAmount::integer), 0) AS points_spent
        FROM Peers AS p
        LEFT JOIN TransferredPoints AS tp1 ON p.Nickname = tp1.CheckedPeer
        GROUP BY p.Nickname
    ) t1
    LEFT JOIN (
        SELECT p.Nickname, COALESCE(SUM(tp2.PointsAmount::integer), 0) AS points_received
        FROM Peers AS p
        LEFT JOIN TransferredPoints AS tp2 ON p.Nickname = tp2.CheckingPeer
        GROUP BY p.Nickname
    ) t2 ON t1.Nickname = t2.Nickname
    ORDER BY points_change DESC;
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM calculate_points_change();
*/ 
	 
-- 5) Посчитать изменение в количестве пир поинтов каждого пира по таблице, возвращаемой первой функцией из Part 3
-- Результат вывести отсортированным по изменению числа поинтов. 
-- Формат вывода: ник пира, изменение в количество пир поинтов
DROP FUNCTION IF EXISTS calculate_points_balance();
CREATE OR REPLACE FUNCTION calculate_points_balance()
RETURNS TABLE (nickname TEXT, points_change BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT t1.Nickname, t2.points_received - t1.points_spent AS points_change
    FROM (
        SELECT p.Nickname, COALESCE(SUM(tp1."PointsAmount"::integer), 0) AS points_spent
        FROM Peers AS p
        LEFT JOIN count_transferred_points() AS tp1 ON p.Nickname = tp1."Peer2"
        GROUP BY p.Nickname
    ) t1
    LEFT JOIN (
        SELECT p.Nickname, COALESCE(SUM(tp2."PointsAmount"::integer), 0) AS points_received
        FROM Peers AS p
        LEFT JOIN count_transferred_points() AS tp2 ON p.Nickname = tp2."Peer1"
        GROUP BY p.Nickname
    ) t2 ON t1.Nickname = t2.Nickname
    ORDER BY points_change DESC;
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM calculate_points_balance();
*/


-- 6) Определить самое часто проверяемое задание за каждый день
-- При одинаковом количестве проверок каких-то заданий в определенный день, вывести их все. 
-- Формат вывода: день, название задания
DROP FUNCTION IF EXISTS popular_task();
CREATE OR REPLACE FUNCTION popular_task()
RETURNS TABLE ("Day" DATE, "Task" TEXT) AS $$
BEGIN
    RETURN QUERY
	WITH A AS (SELECT Task, Checks.Date, COUNT(Checks.Date) AS Ccount FROM Checks
	GROUP BY 2, 1)
	SELECT Date, Task FROM A AS B WHERE Ccount = (SELECT MAX(Ccount) FROM (SELECT * FROM A WHERE A.Date = B.Date) AS C)
	ORDER BY 1;
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM popular_task();
*/


-- 7) Найти всех пиров, выполнивших весь заданный блок задач и дату завершения последнего задания
-- Параметры процедуры: название блока, например "CPP". 
-- Результат вывести отсортированным по дате завершения. 
-- Формат вывода: ник пира, дата завершения блока (т.е. последнего выполненного задания из этого блока)
DROP FUNCTION IF EXISTS all_tasks_in_block(name_of_block TEXT);
CREATE OR REPLACE FUNCTION all_tasks_in_block(name_of_block TEXT)
RETURNS TABLE (Peer TEXT, Day DATE) 
LANGUAGE plpgsql AS 
$$
DECLARE 
    count_of_tasks_in_block INTEGER;
	last_task_in_the_block TEXT;
BEGIN
    -- вычисляем количество заданий в блоке и сохраняем в count_of_tasks_in_block
    SELECT COUNT(*) INTO count_of_tasks_in_block
    FROM tasks
    WHERE title ~ ('^' || name_of_block || '[0-9E]_');
	
	 RAISE NOTICE 'count_of_tasks_in_block: %', count_of_tasks_in_block;
   
	
    -- название последнего задания в блоке сохраняем в 	last_task_in_the_block
	
	SELECT title INTO last_task_in_the_block
    FROM tasks
    WHERE title ~ ('^' || name_of_block || '[0-9E]_')
	ORDER BY 1 DESC
	LIMIT 1;
	
	 RAISE NOTICE 'last_task_in_the_block: %', last_task_in_the_block;
	  RAISE NOTICE 'name_of_block: %', name_of_block;
    
    -- выводим имя пира и дату сдачи последнего задания в блоке
   RETURN QUERY
    SELECT c.peer, MAX(c.date)
    FROM Checks c
    WHERE c.task = last_task_in_the_block  AND EXISTS (
        SELECT 1
        FROM Checks c2
		    JOIN p2p p ON p."Check" = c2.id
        WHERE c2.peer = c.peer AND 
		      c2.task ~ ('^' || name_of_block || '[0-9E]_') AND 
		      p.State = 'success'
        GROUP BY c2.peer
        HAVING COUNT(DISTINCT c2.task) = count_of_tasks_in_block
    )  AND (
             name_of_block NOT LIKE 'C%'
       OR (
         SELECT COUNT (DISTINCT c3.task) 
          FROM Verter v2
              JOIN Checks c3 ON v2."Check" = c3.id
          WHERE c3.peer = c.peer 
            AND c3.task ~ ('^' || name_of_block || '[0-9E]_')
            AND v2.state = 'success'
            ) = 6
        )
    GROUP BY c.peer;
    
END;
$$;

-- Вызов функции
/*
SELECT * FROM all_tasks_in_block('C');
SELECT * FROM all_tasks_in_block('DO');
SELECT * FROM all_tasks_in_block('CPP');
*/

-- 8) Определить, к какому пиру стоит идти на проверку каждому обучающемуся
-- Определять нужно исходя из рекомендаций друзей пира, т.е. нужно найти пира, проверяться у которого рекомендует наибольшее число друзей. 
-- Формат вывода: ник пира, ник найденного проверяющего
DROP FUNCTION IF EXISTS the_best_checker();
CREATE OR REPLACE FUNCTION the_best_checker()
RETURNS TABLE (Peer TEXT, RecommendedPeer TEXT)
LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
	-- формируем таблицу с подсчетом колличества рекомендаций и исключаем рекомендации на проверяемого пира
	WITH recommendations AS (
		SELECT f.Peer1, r.RecommendedPeer, COUNT(*) AS RecommendationCount
        FROM Friends f
        JOIN Recommendations r ON f.Peer2 = r.Peer
		WHERE f.peer1 <> r.RecommendedPeer
        GROUP BY f.Peer1, r.RecommendedPeer
        ORDER BY 1, 3 DESC	
	), 
	-- выбираем строки с максимальным числом рекомендаций для проверяемого пира
	max_counts AS (
    SELECT Peer1, MAX(RecommendationCount) AS MaxRecommendationCount
    FROM recommendations
    GROUP BY Peer1
    )
   -- выводим строки с учётом максимального числа рекомендаций 
    SELECT r.Peer1, r.RecommendedPeer
    FROM recommendations r
    JOIN max_counts ON r.Peer1 = max_counts.Peer1 AND r.RecommendationCount = max_counts.MaxRecommendationCount;

END;
$$;

-- Вызов функции
/*
SELECT * FROM the_best_checker();
*/

-- 9) Определить процент пиров, которые:
-- Приступили только к блоку 1
-- Приступили только к блоку 2
-- Приступили к обоим
-- Не приступили ни к одному
DROP FUNCTION IF EXISTS count_of_peers_started_bloks(name_of_block1 TEXT, name_of_block2 TEXT);
CREATE OR REPLACE FUNCTION count_of_peers_started_bloks(name_of_block1 TEXT, name_of_block2 TEXT)
RETURNS TABLE (StartedBlock1 INTEGER, 
			   StartedBlock2 INTEGER, 
			   StartedBothBlocks INTEGER, 
			   DidntStartAnyBlock INTEGER) 
LANGUAGE plpgsql AS 
$$
DECLARE 
     StartedBlock1 INTEGER; 
	 StartedBlock2 INTEGER; 
	 StartedBothBlocks INTEGER; 
	 DidntStartAnyBlock INTEGER;
BEGIN
--      количесво пиров, приступивших к блоку 1
     SELECT  COUNT (DISTINCT c.peer) INTO StartedBlock1
	 FROM Checks c
	 WHERE c.task ~ ('^' || name_of_block1 || '[0-9E]_');
	 
--      количесво пиров, приступивших к блоку 2
     SELECT  COUNT (DISTINCT c.peer) INTO StartedBlock2
	 FROM Checks c
	 WHERE c.task ~ ('^' || name_of_block2 || '[0-9E]_');
	 
--      количесво пиров, приступивших к обоим блокам
     SELECT  COUNT (DISTINCT c.peer) INTO StartedBothBlocks
	 FROM Checks c
	 WHERE c.task ~ ('^' || name_of_block2 || '[0-9E]_') AND
      c.peer IN (
        SELECT peer
        FROM Checks
        WHERE task ~ ('^' || name_of_block1 || '[0-9E]_')
      );
		   
--      количесво пиров, не приступивших ни к одному блоку
     SELECT  COUNT (nickname) INTO DidntStartAnyBlock
	 FROM peers c
	 WHERE nickname NOT IN (
                        SELECT peer
                        FROM Checks
                        WHERE task ~ ('^' || name_of_block1 || '[0-9E]_')
                           OR task ~ ('^' || name_of_block2 || '[0-9E]_')
          ) OR nickname IN (
			            SELECT nickname
			            FROM peers
			            EXCEPT
			            SELECT peer
			            FROM checks			  
           );
					   
--  выводим полученные данные
     RETURN QUERY
	 SELECT  StartedBlock1,
	         StartedBlock2,
	         StartedBothBlocks,
	         DidntStartAnyBlock;
    
END;
$$;

-- Вызов функции
/*
SELECT* FROM count_of_peers_started_bloks('C', 'CPP');
*/

-- 10) Определить процент пиров, которые когда-либо успешно проходили проверку в свой день рождения
-- Также определите процент пиров, которые хоть раз проваливали проверку в свой день рождения. 
-- Формат вывода: процент пиров, успешно прошедших проверку в день рождения, процент пиров, проваливших проверку в день рождения
DROP FUNCTION IF EXISTS Birthday_Checks();
CREATE OR REPLACE FUNCTION Birthday_Checks()
RETURNS TABLE ("SuccessfulChecks" INTEGER, "UnSuccessfulChecks" INTEGER) AS $$
BEGIN
	RETURN QUERY
	WITH A AS (SELECT Peer, Checks.ID, P2P.State AS p2p_state, Verter.State AS v_state from Checks
					JOIN P2P ON P2P."Check" = Checks.ID
					JOIN Verter on Verter."Check" = Checks.ID
					JOIN Peers ON Peers.Nickname = Checks.Peer
					WHERE EXTRACT(MONTH FROM Checks.Date) = EXTRACT(MONTH FROM  Peers.Birthday)
					AND EXTRACT(DAY FROM Checks.Date) = EXTRACT(DAY FROM Peers.Birthday)),
	f AS (SELECT COUNT(*) AS fails FROM A WHERE p2p_state = 'failure' OR v_state = 'failure'),
	s AS (SELECT COUNT(*) AS succ FROM A WHERE p2p_state = 'success' AND v_state = 'success' OR v_state IS NULL),
	ss AS (SELECT (fails + succ) AS summ FROM f CROSS JOIN s)
	SELECT ((SELECT succ FROM s)::numeric/(SELECT summ FROM ss)*100)::integer AS SuccessfulChecks,
			((SELECT fails FROM f)::numeric/(SELECT summ FROM ss)*100)::integer AS UnSuccessfulChecks;
END
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM Birthday_Checks();
*/

-- 11) Определить всех пиров, которые сдали заданные задания 1 и 2, но не сдали задание 3
-- Параметры процедуры: названия заданий 1, 2 и 3. 
-- Формат вывода: список пиров
DROP FUNCTION IF EXISTS one_two_notthree(task1 TEXT, task2 TEXT, task3 TEXT);
CREATE OR REPLACE FUNCTION one_two_notthree(task1 TEXT, task2 TEXT, task3 TEXT)
RETURNS TABLE ("Peer" TEXT) AS $$
BEGIN
    RETURN QUERY
    WITH task1_completed AS (
        SELECT c.Peer 
        FROM checks c
        LEFT JOIN p2p p ON p."Check" = c.id
        LEFT JOIN verter v ON v."Check" = c.id
        WHERE c.task LIKE task1 
            AND p.state = 'success' 
            AND ((c.Task > 'C6_s21_Matrix') OR (v.state = 'success'))
    ),
    task2_completed AS (
        SELECT c.Peer 
        FROM checks c
        LEFT JOIN p2p p ON p."Check" = c.id
        LEFT JOIN verter v ON v."Check" = c.id
        WHERE c.Task LIKE task2 
            AND p.state = 'success'
            AND ((c.Task > 'C6_s21_Matrix') OR (v.state = 'success'))
    ),
    task3_notcompleted AS (
        SELECT c.Peer 
        FROM checks c
        LEFT JOIN p2p p ON p."Check" = c.id
        LEFT JOIN verter v ON v."Check" = c.id
        WHERE c.Task LIKE task3 
            AND (p.state = 'failure' OR (c.Task > 'C6_s21_Matrix' AND p.state = 'success' AND v.state = 'failure'))
      AND p.state <> 'success'
    ),
    task3_notcompleted_2 AS (
        SELECT q.nickname
        FROM (
            (SELECT nickname FROM peers)
            EXCEPT
            (SELECT c.Peer FROM checks c WHERE c.task LIKE task3)
        ) AS q
    )
    SELECT * FROM (
        (SELECT * FROM task1_completed) 
        INTERSECT
        (SELECT * FROM task2_completed)
        INTERSECT
        ((SELECT * FROM task3_notcompleted)
        UNION
        (SELECT * FROM task3_notcompleted_2))
    ) AS DONE;
END;
$$ LANGUAGE plpgsql;
-- Вызов функции
/*
SELECT * FROM one_two_notthree('DO3_s21_LinuxMonitoring_v1.0', 'DO4_s21_LinuxMonitoring_v2.0', 'DO5_s21_SimpleDocker');
SELECT * FROM one_two_notthree('C4_s21_Math', 'C5_s21_Decimal', 'DO1_s21_Linux');
SELECT * FROM one_two_notthree('C4_s21_Math', 'C3_s21_String+', 'C8_s21_3DViewer_v1.0');
*/

-- 12) Используя рекурсивное обобщенное табличное выражение, для каждой задачи вывести кол-во предшествующих ей задач
-- То есть сколько задач нужно выполнить, исходя из условий входа, чтобы получить доступ к текущей. 
-- Формат вывода: название задачи, количество предшествующих

CREATE OR REPLACE FUNCTION count_of_previous_tasks()
RETURNS TABLE (Task TEXT, PrevCount INTEGER)
LANGUAGE plpgsql AS
$$
BEGIN
    RETURN QUERY
    WITH RECURSIVE prev_tasks AS (
		SELECT title AS Task, parenttask, 0 AS PrevCount
		FROM tasks
		WHERE parenttask IS NULL
		UNION 
		SELECT t.title, t.parenttask, pt.PrevCount + 1
		FROM tasks t
		JOIN prev_tasks pt ON pt.Task = t.parenttask
	)
    
    SELECT prev_tasks.Task, prev_tasks.PrevCount
	FROM prev_tasks;


END;
$$;

-- Вызов функции
/*
SELECT * FROM count_of_previous_tasks();
*/

-- 13) Найти "удачные" для проверок дни. День считается "удачным", если в нем есть хотя бы N идущих подряд успешных проверки
-- Параметры процедуры: количество идущих подряд успешных проверок N. 
-- Временем проверки считать время начала P2P этапа. 
-- Под идущими подряд успешными проверками подразумеваются успешные проверки, между которыми нет неуспешных. 
-- При этом кол-во опыта за каждую из этих проверок должно быть не меньше 80% от максимального. 
-- Формат вывода: список дней

CREATE OR REPLACE FUNCTION find_successful_days(N INTEGER)
RETURNS TABLE (successful_day DATE) AS $$
DECLARE
    searching_date DATE;
    count_of_success_checks INTEGER := 0;
    state_value TEXT;
    time_value DATE;
	check_id INTEGER;
	-- Объявление курсора на два столбца, отсортированных по дате и времени
    cursor_success CURSOR FOR 
        SELECT p2p.State, p2p.Time, p2p."Check"
        FROM p2p 
        WHERE p2p.state <> 'start' 
        ORDER BY p2p.time;
BEGIN
    -- удаление временной таблицы, если она есть
    IF EXISTS (SELECT 1 FROM pg_tables WHERE tablename = 'temp_results') THEN
        EXECUTE 'DROP TABLE temp_results';
    END IF;

    -- Создание временной таблицы
    EXECUTE 'CREATE TEMPORARY TABLE temp_results (day DATE, checks INTEGER, check_id INTEGER)';

    -- Открытие курсора
    OPEN cursor_success;

    -- Извлечение данных из курсора
    LOOP
        FETCH cursor_success INTO state_value, time_value, check_id;
        EXIT WHEN NOT FOUND;

        -- Обработка текущего значения      
        IF state_value = 'success' THEN
            count_of_success_checks := count_of_success_checks + 1;   -- Увеличение счетчика успешных проверок
        ELSE
            count_of_success_checks := 0; -- обнуление счетчика при неуспешной проверке
        END IF;

        -- Проверка изменения даты
        IF searching_date IS NULL OR searching_date <> time_value::DATE THEN
		    -- Сохранение предыдущего значения даты для проверки на изменение
            searching_date := time_value::DATE;
            IF state_value = 'success' THEN
                count_of_success_checks := 1; -- Сброс счетчика успешных проверок до 1 при изменении даты и если первая проверка дня успешная
            ELSE  
                count_of_success_checks := 0; -- Сброс счетчика успешных проверок до 0 при изменении даты и если первая проверка дня неуспешная
            END IF;
        END IF;

        -- Вставка значения даты и количества проверок во временную таблицу
        EXECUTE 'INSERT INTO temp_results VALUES ($1::DATE, $2, $3)' USING time_value, count_of_success_checks, check_id;
    END LOOP;

    -- Закрытие курсора
    CLOSE cursor_success;
    
	-- вывод данных, сгруппированых по дате с выбором максимального значения счетчика
    RETURN QUERY SELECT day
                 FROM temp_results tr
                     JOIN xp ON xp."Check" = tr.Check_id
					 JOIN checks c ON c.id = tr.Check_id
					 JOIN tasks t ON t.title = c.task
				 WHERE checks >= N AND xp.xpamount >= t.maxxp*0.8
                 GROUP BY day;
				 
	-- удаление временной таблицы			 
	 EXECUTE 'DROP TABLE temp_results';
END;
$$ LANGUAGE PLPGSQL;

-- вызов функции
/*
SELECT * FROM find_successful_days(4);
*/

-- 14) Определить пира с наибольшим количеством XP
-- Формат вывода: ник пира, количество XP

DROP FUNCTION IF EXISTS Super_Brain();
CREATE OR REPLACE FUNCTION Super_Brain()
RETURNS TABLE ("Peer" TEXT, "XP" BIGINT) AS $$
BEGIN
    RETURN QUERY
	WITH tmp AS (SELECT Checks.Peer, Task, MAX(XP.XPAmount) AS MXP FROM Checks
				 JOIN XP ON XP."Check" = Checks.ID
				 GROUP BY 1, 2),
			 A AS (SELECT tmp.Peer, SUM(MXP) AS XP FROM tmp GROUP BY 1)	
	SELECT Peer, XP FROM A
	WHERE XP = (SELECT MAX(XP) FROM A);
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM Super_Brain();
*/

-- 15) Определить пиров, приходивших раньше заданного времени не менее N раз за всё время
-- Параметры процедуры: время, количество раз N. 
-- Формат вывода: список пиров
DROP FUNCTION IF EXISTS find_peers_with_early_arrivals(arg1 INTEGER, arg2 TIME);
CREATE OR REPLACE FUNCTION find_peers_with_early_arrivals(arg1 INTEGER, arg2 TIME)
RETURNS TABLE (peer TEXT) AS $$
BEGIN
    IF arg1 >= 0 THEN
        IF arg2 IS NOT NULL THEN
            -- Проверка правильности формата времени
            BEGIN
                PERFORM arg2::TIME;
            EXCEPTION
                WHEN others THEN
                    RAISE EXCEPTION 'Invalid time format';
            END;
           RETURN QUERY
        SELECT t.Peer
        FROM TimeTracking t
        WHERE t.State = 1 AND Time <= arg2
        GROUP BY t.Peer
        HAVING COUNT(*) >= arg1;
        ELSE
            RAISE EXCEPTION 'Second argument cannot be NULL';
        END IF;
    ELSE
        RAISE EXCEPTION 'First argument must be greater than or equal to 0';
    END IF;
END;
$$ LANGUAGE plpgsql;

/*
-- Вызов функции
SELECT * from find_peers_with_early_arrivals(2, '13:59:59');
*/

-- 16) Определить пиров, выходивших за последние N дней из кампуса больше M раз
-- Параметры процедуры: количество дней N, количество раз M. 
-- Формат вывода: список пиров
DROP FUNCTION IF EXISTS counting_exits(number_of_days INTEGER, number_outs INTEGER);
CREATE OR REPLACE FUNCTION counting_exits(number_of_days INTEGER, number_outs INTEGER)
RETURNS TABLE ("Peer" TEXT) AS $$
BEGIN
	IF number_of_days > 0 AND number_outs > 0 THEN
    RETURN QUERY
    SELECT Peer
    FROM TimeTracking
    WHERE State = 2
    GROUP BY Peer
    HAVING COUNT(DISTINCT Date) > number_of_days AND COUNT(*) > number_outs;
	ELSE
        RAISE EXCEPTION 'Arguments must be greater than 0';
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM counting_exits(1, 1);
*/

-- 17) Определить для каждого месяца процент ранних входов
DROP FUNCTION IF EXISTS get_early_entries_stats();
CREATE OR REPLACE FUNCTION get_early_entries_stats()
RETURNS TABLE (Month TEXT, EarlyEntries INTEGER) AS $$
BEGIN
  RETURN QUERY
  WITH month_table AS (
    SELECT generate_series(DATE '2000-01-01', DATE '2000-12-01', '1 month') AS month
  )
  SELECT 
      t_data.month_of_birth AS "Month",
      CAST(COALESCE((SUM(t_data.early_visit) * 100) / NULLIF(SUM(t_data.all_visit), 0), 0) AS INTEGER) AS "EarlyEntries"
  FROM (
      SELECT 
          TO_CHAR(mt.month, 'Month') AS month_of_birth,
          pc.nickname,
          SUM(pc.visit_counter) AS all_visit,
          SUM(pc.early_visit) AS early_visit
      FROM 
          month_table mt
      LEFT JOIN (
          SELECT 
              TO_CHAR(p.birthday, 'Month') AS month_of_birth,
              p.nickname,
              COUNT(CASE WHEN t.state = 1 THEN 1 END) AS visit_counter,
              COUNT(CASE WHEN t.state = 1 AND t.time < '12:00:00' THEN 1 END) AS early_visit
          FROM peers AS p
          LEFT JOIN timetracking t ON p.nickname = t.peer
          GROUP BY p.nickname, p.birthday
      ) AS pc ON TO_CHAR(mt.month, 'Month') = pc.month_of_birth
      GROUP BY mt.month, pc.nickname
      
  ) AS t_data
  GROUP BY t_data.month_of_birth
  ORDER BY EXTRACT(MONTH FROM to_date(t_data.month_of_birth, 'Month'));

  RETURN;
END;
$$ LANGUAGE plpgsql;

-- Вызов функции
/*
SELECT * FROM get_early_entries_stats();
*/

