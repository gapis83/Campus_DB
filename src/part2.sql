-- Тестовые запросы/вызовы для проверки процедур и триггеров
/*
-- 1)
CALL add_check('Legolas_Greenleaf', 'Aragorn_son_of_Arathorn', 'C1_s21_Pool', 'start', NOW());
SELECT * FROM Checks ORDER BY 1 DESC LIMIT 1; 

CALL add_check('Legolas_Greenleaf', 'Aragorn_son_of_Arathorn', 'C1_s21_Pool', 'success', NOW());
SELECT * FROM p2p ORDER BY 1 DESC LIMIT 2;

--2)
CALL add_verter('Legolas_Greenleaf', 'C1_s21_Pool', 'success', NOW());
SELECT * FROM verter ORDER BY 1 DESC LIMIT 2; -- запись в таблицу Verter добавлена

--3)
SELECT * FROM TransferredPoints; -- запись в TransferredPoints добавлена при выполнении первой проверки

--4)
INSERT INTO XP (id, "Check", XPAmount) VALUES ((SELECT MAX(id) FROM xp)+1, 69, 1300);
SELECT * FROM xp WHERE id = (SELECT MAX(id) FROM xp);

-- 1)
CALL add_check('Frodo_Baggins', 'Aragorn_son_of_Arathorn', 'C1_s21_Pool', 'start', NOW());
SELECT * FROM Checks ORDER BY 1 DESC LIMIT 1; 

CALL add_check('Frodo_Baggins', 'Aragorn_son_of_Arathorn', 'C1_s21_Pool', 'success', NOW());	
SELECT * FROM p2p ORDER BY 1 DESC LIMIT 2;

--2)
CALL add_verter('Frodo_Baggins', 'C1_s21_Pool', 'success', NOW());
SELECT * FROM verter ORDER BY 1 DESC LIMIT 2; -- запись в таблицу Verter добавлена

--3)
SELECT * FROM TransferredPoints; -- запись в TransferredPoints добавлена при выполнении первой проверки

--4)
INSERT INTO xp (id, "Check", xpamount) VALUES ((SELECT MAX(id) FROM xp)+1, 70, 1300);
SELECT * FROM xp WHERE id = (SELECT MAX(id) FROM xp);
*/


-- Процедура добавления P2P проверки

CREATE OR REPLACE PROCEDURE add_check(
    IN p_checked_nickname text,
    IN p_checking_nickname text,
    IN p_task_title text, 
    IN p_check_status check_status, 
    IN p_time timestamp with time zone
)
LANGUAGE plpgsql
AS $$
DECLARE
  p_check_id INTEGER;
  p2p_check_id INTEGER;
  start_checks_count INTEGER;
  end_checks_count INTEGER;
  p_p2p_id INTEGER;
BEGIN
	
-- Подсчёт количества начатых и завершенных проверок и сохранение полученных значений в переменные
  SELECT COUNT(*) INTO start_checks_count
  FROM P2P
  JOIN Checks ON p2p."Check" = Checks.ID
  WHERE Checks.Peer = p_checked_nickname
    AND P2P.CheckingPeer = p_checking_nickname
    AND Checks.Task = p_task_title
    AND P2P.State = 'start';

  SELECT COUNT(*) INTO end_checks_count
  FROM P2P
  JOIN Checks ON "Check" = Checks.ID
  WHERE Checks.Peer = p_checked_nickname
    AND P2P.CheckingPeer = p_checking_nickname
    AND Checks.Task = p_task_title
    AND P2P.State <> 'start';

-- Переменная, хранящая id проверки для проверяемого и задания, указанных в переменных процедуры 
  SELECT ID INTO p2p_check_id
  FROM Checks 
  WHERE Peer = p_checked_nickname
    AND Task = p_task_title;

-- Если пир проверяет сам себя - ошибка
  IF p_checked_nickname = p_checking_nickname THEN
      RAISE EXCEPTION 'Error: p_checked_nickname cannot be equal to p_checking_nickname.';
      RETURN;
  END IF;
  
-- Проверка корректности количества начатых и завершенных проверок проекта
  IF (p_check_status = 'start')
  OR (p_check_status <> 'start' AND p2p_check_id IS NOT NULL) THEN
      IF (p_check_status = 'start' AND 
          start_checks_count <> end_checks_count)
      OR
         (p_check_status <> 'start' 
      AND
          NOT EXISTS (SELECT id FROM p2p 
          WHERE "Check" = p_check_id 
          AND state = 'start' AND
          CheckingPeer = p_checking_nickname)
      AND
          start_checks_count -  end_checks_count <> 1) THEN
          RAISE EXCEPTION 'Error: condition not met that after adding data, the number of started checks is equal to or greater by 1 than the number of completed checks';
      ELSE
-- Добавить новую запись в таблицу "checks" только если статус "start"
  IF p_check_status = 'start' THEN
      SELECT COALESCE(MAX(id), 0) + 1 INTO p_check_id -- !!!!!!!!!!!!!!!!!!!! Добавил эти две строки, для выяснения следующего ID
      FROM Checks;

      INSERT INTO Checks (id, Peer, Task, Date)
      VALUES (p_check_id, p_checked_nickname, p_task_title, CURRENT_DATE)
      RETURNING ID INTO p_check_id;
  ELSE
      SELECT Checks.ID INTO p_check_id
      FROM Checks
      JOIN P2P ON Checks.ID = P2P."Check"
      WHERE Checks.Peer = p_checked_nickname
      AND P2P.CheckingPeer = p_checking_nickname
      AND Checks.Task = p_task_title
      GROUP BY Checks.ID
      HAVING COUNT(Checks.ID) = 1;
  END IF;
  
-- Добавить запись в таблицу P2P
  IF p_check_status <> 'start' 
    AND p_time < (SELECT Time FROM P2P WHERE "Check" = p_check_id) THEN
    RAISE EXCEPTION 'Error: check cannot start after completion.';
    RETURN;
  END IF;
   SELECT COALESCE(MAX(id), 0) + 1 INTO p_p2p_id -- !!!!!!!!!!!!!!!!!!!! Добавил эти две строки, для выяснения следующего ID
      FROM p2p;
  INSERT INTO P2P (id, "Check", CheckingPeer, State, Time)
  VALUES ( p_p2p_id, p_check_id, p_checking_nickname, p_check_status, p_time);
      END IF;
  ELSE
-- Если запись не существует, выдать уведомление (NOTICE) и завершить процедуру
  RAISE NOTICE 'Error: the check you are trying to add does not have a corresponding entry in the checks table';
  RETURN;
  END IF;
END;
$$;


-- Процедура добавления данных в таблицу Verter

CREATE OR REPLACE PROCEDURE add_verter(
    IN p_checked_nickname text, 
    IN p_task VARCHAR(50),
    IN p_state check_status,
    IN p_time timestamp with time zone)
LANGUAGE plpgsql
AS $$
DECLARE
  current_check_id INTEGER;
  p_check_id INTEGER;
BEGIN
-- Выбираем последнюю проверку для данного Пира и задания
  SELECT c.id INTO current_check_id
  FROM Checks c
  JOIN P2P p ON p."Check" = c.ID
  WHERE c.Peer = p_checked_nickname
        AND c.Task = p_task
        AND p.State = 'success'
  ORDER BY c.Date DESC
  LIMIT 1;
  
-- Проверяем, есть ли проверка со статусом "Успешно"
  IF current_check_id IS NULL THEN
    RAISE EXCEPTION 'Not found check with status "success"';
  ELSE
-- Проверяем, не проводилась ли проверка Вертером для текущего check_id
    IF EXISTS (SELECT 1 FROM Verter WHERE "Check" = current_check_id) THEN
      RAISE EXCEPTION 'Check with current check_id already exists in the "verter" table';
    ELSE
-- Если всё ок - вносим две записи в таблицу: Начало проверки и итог проверки.
      SELECT COALESCE(MAX(id), 0) + 1 INTO p_check_id 
      FROM verter;

      INSERT INTO Verter (id,"Check", State, Time)
      VALUES (p_check_id, current_check_id, 'start', p_time);
    
      INSERT INTO Verter (id,"Check", State, Time)
      VALUES (p_check_id +1, current_check_id, p_state, p_time);
    END IF;
  END IF;
END;
$$;


-- Триггер для переноса поинтов

CREATE OR REPLACE FUNCTION trg_Transfer_Points()
RETURNS TRIGGER AS $$
DECLARE
  peer_checked text;
  peer_checking text;
  count_p2p INTEGER;
  state_of_check VARCHAR(15);
  t_trpnt_id INTEGER;
BEGIN
  -- Получаем пару проверяющий/проверяемый и значение state последнего внесённого изменения
  SELECT c.Peer, p.CheckingPeer, p.State INTO peer_checked, peer_checking, state_of_check
  FROM Checks c
  JOIN P2P p ON p."Check" = c.ID 
  WHERE c.ID = NEW."Check"
  ORDER BY p.id DESC
  LIMIT 1;
  
   
  -- Проверяем, есть ли пара проверяющий/проверяемый в таблице transferred_points
  SELECT COUNT(*) INTO count_p2p
  FROM TransferredPoints
  WHERE CheckingPeer = peer_checking
        AND CheckedPeer = peer_checked;
  
  -- если запись существует и состояние проверки 'start', добавляем 1 поинт
  IF state_of_check = 'start' THEN
    IF count_p2p > 0 THEN
	
	  RAISE NOTICE 'Триггер trg_Transfer_Points: Обновлено значение PointsAmount для пары пиров (%, %)', peer_checking, peer_checked;
	  
      UPDATE TransferredPoints
      SET PointsAmount = pointsamount + 1
      WHERE CheckingPeer = peer_checking
            AND CheckedPeer = peer_checked;
    ELSE
      -- если записи нет, добавляем новую строку и устанавливаем количество поинтов = 1
	  SELECT COALESCE(MAX(id), 0) + 1 INTO t_trpnt_id -- !!!!!!!!!!!!!!!!!!!! Добавил эти две строки, для выяснения следующего ID
      FROM TransferredPoints;
      INSERT INTO TransferredPoints (id, CheckingPeer, CheckedPeer, PointsAmount)
      VALUES (t_trpnt_id, peer_checking, peer_checked, 1);
	  
	  RAISE NOTICE 'Триггер trg_Transfer_Points: Вставлена новая запись с PointsAmount = 1 для пары пиров (%, %)', peer_checking, peer_checked;
	  
    END IF;
  END IF;
  
  -- Возвращаем значение типа TRIGGER
  RETURN NEW;
END;

$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_transfer_points_trigger
AFTER INSERT ON p2p 
FOR EACH ROW
EXECUTE FUNCTION trg_Transfer_Points();


-- Триггер для добавления записи в таблицу ХР

CREATE OR REPLACE FUNCTION trg_CheckXP()
RETURNS TRIGGER AS $$
DECLARE
  maxXP INT;
  VcheckStatus VARCHAR(255);
  P2PcheckStatus VARCHAR(255);
BEGIN
-- Получаем максимальное доступное количество XP для задачи
  SELECT t.MaxXP INTO maxXP 
  FROM Checks c
  JOIN Tasks t ON c.Task = t.Title 
  WHERE c.ID = NEW."Check";
  
-- Получаем значение поля Check для проверяемой записи в таблице Verter
  SELECT v.State INTO VcheckStatus 
  FROM Checks c
  JOIN Verter v ON v."Check" = c.ID
  WHERE c.ID = NEW."Check" AND v.state <> 'start';
  
-- Получаем значение поля Check для проверяемой записи в таблице P2P
  SELECT p.State INTO P2PcheckStatus 
  FROM Checks c
  JOIN p2p p ON p."Check" = c.ID
  WHERE c.ID = NEW."Check" AND p.state <> 'start';
   
-- Проверяем условия корректности записи
  IF NEW.XPAmount <= maxXP AND NEW.XPAmount > 0 AND (VcheckStatus = 'success' OR VcheckStatus IS NULL) AND P2PcheckStatus = 'success' THEN
    RETURN NEW; -- Запись корректна, продолжаем добавление
  ELSE
    RAISE EXCEPTION 'Некорректная запись XP'; -- Запись не прошла проверку, отменяем добавление
  END IF;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_CheckXP
BEFORE INSERT ON XP
FOR EACH ROW
EXECUTE FUNCTION trg_CheckXP();
