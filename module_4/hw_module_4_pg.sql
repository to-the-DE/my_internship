-- Создаем таблицу users
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Создаем таблицу users_audit 
CREATE TABLE users_audit (
    id SERIAL PRIMARY KEY,
    user_id INTEGER,
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    changed_by TEXT,
    field_changed TEXT,
    old_value TEXT,
    new_value TEXT
);

-- № 1. Создайте функцию логирования изменений по трем полям.
CREATE OR REPLACE FUNCTION audit_user_changes() RETURNS TRIGGER AS $$
DECLARE
BEGIN 
	IF NEW.name IS DISTINCT FROM OLD.name THEN
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (OLD.id, current_user, 'name', OLD.name, NEW.name);
	END IF;

	IF NEW.email IS DISTINCT FROM OLD.email THEN
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (OLD.id, current_user, 'email', OLD.email, NEW.email);
	END IF;
	
	IF NEW.role IS DISTINCT FROM OLD.role THEN
		INSERT INTO users_audit(user_id, changed_by, field_changed, old_value, new_value)
		VALUES (OLD.id, current_user, 'role', OLD.role, NEW.role);
	END IF;
	
	RETURN NEW;	
END;
$$ LANGUAGE plpgsql;

-- № 2. Создайте trigger на таблицу users.
CREATE TRIGGER trigger_audit_user_changes
BEFORE UPDATE ON users
FOR EACH ROW
EXECUTE FUNCTION audit_user_changes();

-- № 3. Установите расширение pg_cron.
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- № 4. Создайте функцию, которая будет доставать только свежие данные (за сегодняшний день)
--      и будет сохранять их в образе Docker по пути /tmp/users_audit_export_, а далее указывает
--      ту дату, за который этот csv был создан.
CREATE OR REPLACE FUNCTION export_audit_to_csv() RETURNS void AS $outer$
DECLARE
	PATH TEXT := '/tmp/users_audit_export_' || to_char(now(), 'YYYYMMDD_HH24MI') || '.csv';
BEGIN
	EXECUTE format(
		$inner$
		COPY (
			SELECT user_id, changed_at, changed_by, field_changed, old_value, new_value
			FROM users_audit
			WHERE changed_at >= NOW() - INTERVAL '1 day'
			ORDER BY changed_at
		) TO '%s' WITH CSV HEADER
		$inner$, path
	);
END;
$outer$ LANGUAGE plpgsql;

-- № 5. Установите планировщик pg_cron на 3:00 ночи.
SELECT cron.schedule(
	job_name := 'daily_audit_export',
	schedule := '0 3 * * *',
	command := $$select export_audit_to_csv();$$
	);

-- проверяем, что планировщик запущен
SELECT * FROM cron.job;
