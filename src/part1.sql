/*
DROP TABLE IF EXISTS Peers, Tasks, Checks, P2P, Verter CASCADE;
DROP TABLE IF EXISTS Recommendations, Friends, TimeTracking, TransferredPoints, XP, CASCADE;
DROP TYPE check_status CASCADE;
DROP PROCEDURE IF EXISTS import_csv_data, export_csv_data
 */
 -- Создание таблицы "Peers"
CREATE TABLE Peers (
  Nickname TEXT PRIMARY KEY,
  Birthday DATE
);

-- Создание таблицы "tasks"
CREATE TABLE Tasks (
  Title TEXT PRIMARY KEY,
  ParentTask TEXT REFERENCES Tasks(Title),
  MaxXP INTEGER NOT NULL
);

ALTER TABLE Tasks ADD CONSTRAINT tasks_parent_task_key
  CHECK (
    CASE
      WHEN Title = 'C1_s21_Pool' THEN ParentTask IS NULL
      ELSE ParentTask IS NOT NULL
    END
  );

ALTER TABLE Tasks ADD CONSTRAINT tasks_parent_task_not_self
CHECK (ParentTask <> Title);

-- Создание типа перечисления "check_status"
CREATE TYPE check_status AS ENUM ('start', 'success', 'failure');

-- Создание таблицы "checks"
CREATE TABLE Checks (
  ID SERIAL PRIMARY KEY,
  Peer TEXT REFERENCES Peers(Nickname) NOT NULL,
  Task TEXT REFERENCES Tasks(Title),
  date DATE NOT NULL
);

-- Создание таблицы "P2P"
CREATE TABLE P2P (
  ID SERIAL PRIMARY KEY,
  "Check" INTEGER REFERENCES Checks(ID) NOT NULL,
  CheckingPeer TEXT REFERENCES Peers(Nickname) NOT NULL,
  State check_status NOT NULL,
  Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE ("Check", CheckingPeer, State)
);

-- Создание таблицы "Verter"
CREATE TABLE verter (
  ID SERIAL PRIMARY KEY,
  "Check" INTEGER REFERENCES Checks(ID) NOT NULL,
  State check_status NOT NULL,
  Time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создание таблицы "TransferredPoints"
CREATE TABLE TransferredPoints (
ID SERIAL PRIMARY KEY,
CheckingPeer TEXT REFERENCES Peers(Nickname) NOT NULL,
CheckedPeer TEXT REFERENCES Peers(Nickname) NOT NULL,
PointsAmount INTEGER NOT NULL
);

-- Создание таблицы "Friends"
CREATE TABLE Friends (
ID SERIAL PRIMARY KEY,
Peer1 TEXT REFERENCES Peers(Nickname) NOT NULL,
Peer2 TEXT REFERENCES Peers(Nickname) NOT NULL,
UNIQUE (Peer1, Peer2)
);

-- Создание таблицы "Recommendations"
CREATE TABLE recommendations (
ID SERIAL PRIMARY KEY,
Peer TEXT REFERENCES Peers(Nickname) NOT NULL,
RecommendedPeer TEXT REFERENCES Peers(Nickname) NOT NULL
);

-- Создание таблицы "XP"
CREATE TABLE XP (
ID SERIAL PRIMARY KEY,
"Check" INTEGER REFERENCES Checks(ID) NOT NULL,
XPAmount INTEGER NOT NULL
);

-- Создание таблицы "TimeTracking"
CREATE TABLE TimeTracking (
ID SERIAL PRIMARY KEY,
Peer TEXT REFERENCES Peers(Nickname) NOT NULL,
Date DATE NOT NULL,
Time TIME NOT NULL,
State INTEGER NOT NULL CHECK (State IN (1, 2))
);
-- Проверка данных, добавляемых в таблицу "TimeTracking"
CREATE OR REPLACE FUNCTION validate_time_tracking()
RETURNS TRIGGER AS $$
DECLARE
    same_peer_state_1_count INTEGER;
    same_peer_state_2_count INTEGER;
BEGIN
-- Подсчёт количества строк с одинаковым значением Peer и состояниями State = 1 и State = 2
    SELECT COUNT(*) INTO same_peer_state_1_count
    FROM TimeTracking
    WHERE Peer = NEW.Peer AND State = 1;

    SELECT COUNT(*) INTO same_peer_state_2_count
    FROM TimeTracking
    WHERE Peer = NEW.Peer AND State = 2;
-- Поиск строки с наибольшей датой и временем для данного Peer
-- и сравнение ее с датой и временем добавляемой строки
    IF 
    (NEW.State = 1 AND same_peer_state_1_count <> same_peer_state_2_count) OR
    (NEW.State = 2 AND (same_peer_state_1_count - same_peer_state_2_count) != 1)
    THEN
-- Если найдена строка с более поздней датой и временем, неверно указан статус (State) 
-- откатываем добавление новой строки
        RAISE EXCEPTION 'Cannot insert. There is a row with a later date and time for the same node, or the state is incorrect';
    END IF;
    RETURN NEW;
	END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER time_tracking_trigger
BEFORE INSERT OR UPDATE ON TimeTracking
FOR EACH ROW
EXECUTE FUNCTION validate_time_tracking();


-- Процедура импорта данных из csv-файла в таблицу базы данных
CREATE OR REPLACE PROCEDURE import_csv_data(
    IN table_name TEXT,
    IN delimiter TEXT
) LANGUAGE plpgsql AS $$
DECLARE
    file_path TEXT;
    output_filename TEXT;
BEGIN
    SELECT CASE
        WHEN version() LIKE '%darwin%' THEN
			current_setting('data_directory') || '/data_folder/' || table_name || '_data.csv'
-- 			'/Users/belvajor/data_folder/'|| table_name ||'_data.csv'
        WHEN version() LIKE '%Ubuntu%' THEN
            '/var/lib/postgresql/data_folder/' || table_name || '_data.csv'
        ELSE
            'C:\\Program Files\\PostgreSQL\\15\\data\\' || table_name || '_data.csv'
        END
    INTO file_path;
    
    EXECUTE format(
        'COPY %I FROM %L DELIMITER %L CSV HEADER',
        table_name,
        file_path,
        delimiter
    );
END;
$$;
-- Импорт данных для таблицы peers, разделитель - запятая
CALL import_csv_data('peers', ',');
-- Импорт данных для таблицы tasks, разделитель - запятая
CALL import_csv_data('tasks', ',');
-- Импорт данных для таблицы checks, разделитель - запятая
CALL import_csv_data('checks', ',');
-- Импорт данных для таблицы p2p, разделитель - запятая
CALL import_csv_data('p2p', ',');
-- Импорт данных для таблицы xp, разделитель - запятая
CALL import_csv_data('xp', ',');
-- Импорт данных для таблицы timetracking, разделитель - запятая
CALL import_csv_data('timetracking', ',');
-- Импорт данных для таблицы recommendations, разделитель - запятая
CALL import_csv_data('recommendations', ',');
-- Импорт данных для таблицы friends, разделитель - запятая
CALL import_csv_data('friends', ',');
-- Импорт данных для таблицы transferredpoints, разделитель - запятая
CALL import_csv_data('transferredpoints', ',');
-- Импорт данных для таблицы verter, разделитель - запятая
CALL import_csv_data('verter', ',');


-- Процедура экспорта данных из csv-файла в таблицу базы данных
CREATE OR REPLACE PROCEDURE export_csv_data(
    IN table_name TEXT,
    IN delimiter TEXT
) LANGUAGE plpgsql AS $$
DECLARE
    file_path TEXT;
    output_filename TEXT;
BEGIN
    SELECT CASE
        WHEN version() LIKE '%darwin%' THEN
            current_setting('data_directory') || '/data_folder/' || table_name || '_out.csv'
-- 			'/Users/belvajor/data_folder/'|| table_name ||'_data.csv'
        WHEN version() LIKE '%Ubuntu%' THEN
            '/var/lib/postgresql/data_folder' || table_name || '_out.csv'
        ELSE
            'C:\\Program Files\\PostgreSQL\\15\\data\\' || table_name || '_out.csv'
        END
    INTO file_path;
    
    EXECUTE format(
        'COPY %I TO %L DELIMITER %L CSV HEADER',
        table_name,
        file_path,
        delimiter
    );
END;
$$;

-- Экспорт данных для таблицы peers, разделитель - запятая
CALL export_csv_data('peers', ',');
-- Экспорт данных для таблицы checks, разделитель - запятая
CALL export_csv_data('checks', ',');
-- Экспорт данных для таблицы p2p, разделитель - запятая
CALL export_csv_data('p2p', ',');
-- Экспорт данных для таблицы xp, разделитель - запятая
CALL export_csv_data('xp', ',');
-- Экспорт данных для таблицы timetracking, разделитель - запятая
CALL export_csv_data('timetracking', ',');
-- Экспорт данных для таблицы recommendations, разделитель - запятая
CALL export_csv_data('recommendations', ',');
-- Экспорт данных для таблицы friends, разделитель - запятая
CALL export_csv_data('friends', ',');
-- Экспорт данных для таблицы transferredpoints, разделитель - запятая
CALL export_csv_data('transferredpoints', ',');
-- Экспорт данных для таблицы verter, разделитель - запятая
CALL export_csv_data('verter', ',');
-- Экспорт данных для таблицы tasks, разделитель - запятая
CALL export_csv_data('tasks', ',');
