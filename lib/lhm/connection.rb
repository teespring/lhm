module Lhm
  class Connection < SimpleDelegator
    LONG_QUERY_TIME_THRESHOLD = 10
    INITIALIZATION_DELAY = 2
    TRIGGER_MAXIMUM_DURATION = 2
    SESSION_WAIT_LOCK_TIMEOUT = LONG_QUERY_TIME_THRESHOLD + INITIALIZATION_DELAY + TRIGGER_MAXIMUM_DURATION
    TABLES_WITH_LONG_QUERIES = %w(designs campaigns campaign_roots tags orders).freeze

    def execute_metadata_locking_statements(statements, table, on_error)
      kill_long_running_queries_on_origin_table!(table)
      with_transaction_timeout(on_error: on_error) do
        statements.each do |statement|
          kill_long_running_queries_during_transaction(table) do
            execute(statement)
          end
        end
      end
    end

    def with_transaction_timeout(on_error: nil)
      lock_wait_timeout = ar_connection.execute("SHOW SESSION VARIABLES WHERE VARIABLE_NAME='LOCK_WAIT_TIMEOUT'").to_a.flatten[1].to_i
      ar_connection.execute("SET SESSION LOCK_WAIT_TIMEOUT=#{SESSION_WAIT_LOCK_TIMEOUT}")
      Lhm.logger.info "Set transaction timeout (SESSION LOCK_WAIT_TIMEOUT) to #{SESSION_WAIT_LOCK_TIMEOUT} seconds."
      yield
    rescue => e
      if on_error.present?
        if e.message =~ /Lock wait timeout exceeded/
          on_error.call("Transaction took more than #{SESSION_WAIT_LOCK_TIMEOUT} seconds (SESSION_WAIT_LOCK_TIMEOUT) to run.. ABORT! #{e.message}")
        else
          on_error.call(e.message)
        end
      else
        raise
      end
    ensure
      ar_connection.execute("SET SESSION LOCK_WAIT_TIMEOUT=#{lock_wait_timeout}")
      Lhm.logger.info "Set transaction timeout (SESSION LOCK_WAIT_TIMEOUT) back to #{lock_wait_timeout} seconds."
    end

    def kill_long_running_queries_on_origin_table!(table, connection: nil)
      return unless killing_queries_enabled? && usually_has_long_queries?(table)

      connection ||= ar_connection
      long_running_queries(table.name, connection: connection).each do |id, query, duration|
        Lhm.logger.info "Action on table #{table.name} detected; killing #{duration}-second query: #{query}."
        begin
          connection.execute("KILL #{id};")
        rescue => e
          if e.message =~ /Unknown thread id/
            Lhm.logger.info "Race condition detected. Process to kill no longer exists. Proceeding despite the following error: #{e.message}"
          else
            raise e
          end
        end
      end
    end

    def kill_long_running_queries_during_transaction(table)
      t = Thread.new do
        if killing_queries_enabled?
          # the goal of this thread is to wait until long running queries that started between
          # #kill_long_running_queries_on_origin_table! and trigger creation
          # to pass the threshold time and then kill them
          sleep(LONG_QUERY_TIME_THRESHOLD + INITIALIZATION_DELAY)
          new_connection = ActiveRecord::Base.connection

          kill_long_running_queries_on_origin_table!(table, connection: new_connection)
        end
      end
      yield
      t.join
    end

    def killing_queries_enabled?
      ENV['LHM_KILL_LONG_RUNNING_QUERIES'] == 'true'
    end

    def usually_has_long_queries?(table)
      TABLES_WITH_LONG_QUERIES.include? table.name
    end

    def long_running_queries(table_name, connection: nil)
      connection ||= ar_connection
      result = connection.execute <<-SQL.strip_heredoc
        SELECT ID, INFO, TIME FROM INFORMATION_SCHEMA.PROCESSLIST
        WHERE command <> 'Sleep'
          AND INFO LIKE '%`#{table_name}`%'
          AND INFO NOT LIKE '%TRIGGER%'
          AND INFO NOT LIKE "%INFORMATION_SCHEMA.PROCESSLIST%"
          AND TIME > '#{LONG_QUERY_TIME_THRESHOLD}'
      SQL
      result.to_a.compact
    end

    private

    def ar_connection
      __getobj__
    end
  end
end