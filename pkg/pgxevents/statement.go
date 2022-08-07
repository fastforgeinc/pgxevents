package pgxevents

import "fmt"

const channel = "pgxevents_event"

func listen() string {
	return fmt.Sprintf("LISTEN %s", channel)
}

func procedure() string {
	return fmt.Sprintf(`
		CREATE OR REPLACE FUNCTION pgxevents_notify_event() RETURNS TRIGGER AS $$

		DECLARE
				data json;
				notification json;

		BEGIN

				-- Convert the old or new row to JSON, based on the kind of action.
				-- Action = DELETE?             -> OLD row
				-- Action = INSERT or UPDATE?   -> NEW row
				IF (TG_OP = 'DELETE') THEN
						data = row_to_json(OLD);
				ELSE
						data = row_to_json(NEW);
				END IF;

				-- Contruct the notification as a JSON string.
				notification = json_build_object(
													'table', TG_TABLE_NAME,
													'action', TG_OP,
													'data', data::text);


				-- Execute pg_notify(channel, notification)
				PERFORM pg_notify('%s', notification::text);

				-- Result is ignored since this is an AFTER trigger
				RETURN NULL;
		END;

		$$ LANGUAGE plpgsql;
	`, channel)
}

func trigger(table string) string {
	command := `
			CREATE OR REPLACE TRIGGER %s
			AFTER INSERT OR UPDATE OR DELETE ON %s
			FOR EACH ROW EXECUTE PROCEDURE pgxevents_notify_event();
		`
	name := fmt.Sprintf("%s_events", table)
	return fmt.Sprintf(command, name, table)
}
