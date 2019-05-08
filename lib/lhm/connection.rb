module Lhm
  class Connection < SimpleDelegator
    TABLES_WITH_LONG_QUERIES = %w(designs campaigns campaign_roots tags orders).freeze

    attr_reader :long_query_threshold, :statement_initialization_delay, :max_statement_duration

    def initialize(delegate, long_query_threshold: 10, statement_initialization_delay: 2, max_statement_duration: 2)
      super(delegate)
      @long_query_threshold = long_query_threshold
      @statement_initialization_delay = statement_initialization_delay
      @max_statement_duration = max_statement_duration
    end

    def metadata_lock_wait_timeout
      @long_query_threshold + @statement_initialization_delay + @max_statement_duration
    end

    def execute_metadata_locking_statements(statements, table, on_error = nil)
      kill_long_running_queries(table) if usually_has_long_queries?(table)
      with_transaction_timeout(on_error: on_error) do
        statements.each do |statement|
          kill_long_blocking_queries_while_statement_is_running(table) do
            execute(statement)
          end
        end
      end
    end

    private

    def ar_connection
      __getobj__
    end

    def get_session_timeout
      ar_connection.select_one("SHOW SESSION VARIABLES LIKE 'LOCK_WAIT_TIMEOUT'")["Value"].to_i
    end

    def set_session_timeout(new_timeout)
      ar_connection.execute("SET SESSION LOCK_WAIT_TIMEOUT=#{new_timeout}")
      Lhm.logger.info "Set transaction timeout (SESSION LOCK_WAIT_TIMEOUT) to #{new_timeout} seconds."
    end

    def kill_long_blocking_queries_while_statement_is_running(table)
      t = Thread.new do
        if killing_queries_enabled?
          # the goal of this thread is to kill queries on the table specified that may be blocking metadata_lock
          # acquisition. These queries started before the current metadata locking statement started
          # We delay query killing to confirm the statement we want to protect got actually blocked.
          sleep(@long_query_threshold + @statement_initialization_delay)
          new_connection = ActiveRecord::Base.connection

          kill_long_running_queries(table, connection: new_connection)
        end
      end
      yield

      # When we get to this line the statement we are trying to protect is already done. That means we don't need to
      # wait for the query killing thread to complete nor we need to kill more queries.
      t.kill
    end

    def kill_long_running_queries(table, connection: nil)
      return unless killing_queries_enabled?

      connection ||= ar_connection
      long_running_queries(table.name, connection).each do |id, query, duration|
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

    def with_transaction_timeout(on_error: nil)
      lock_wait_timeout = get_session_timeout
      set_session_timeout(metadata_lock_wait_timeout)
      yield
    rescue => e
      if on_error.present?
        if e.message =~ /Lock wait timeout exceeded/
          on_error.call("Transaction took more than #{metadata_lock_wait_timeout} seconds (SESSION_WAIT_LOCK_TIMEOUT) to run.. ABORT! #{e.message}")
        else
          on_error.call(e.message)
        end
      else
        raise
      end
    ensure
      set_session_timeout(lock_wait_timeout)
    end

    def killing_queries_enabled?
      ENV['LHM_KILL_LONG_RUNNING_QUERIES'] == 'true'
    end

    def usually_has_long_queries?(table)
      TABLES_WITH_LONG_QUERIES.include? table.name
    end

    def long_running_queries(table_name, connection)
      result = connection.execute <<-SQL.strip_heredoc
        SELECT ID, INFO, TIME FROM INFORMATION_SCHEMA.PROCESSLIST
        WHERE command <> 'Sleep'
          AND INFO LIKE '%`#{table_name}`%'
          AND INFO NOT LIKE '%large hadron migration%'
          AND INFO NOT LIKE "%INFORMATION_SCHEMA.PROCESSLIST%"
          AND TIME > '#{@long_query_threshold}'
      SQL
      result.to_a.compact
    end
  end
end