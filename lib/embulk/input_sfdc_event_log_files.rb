require 'httpclient'
require 'json'
require 'time'
require 'csv'

module Embulk

  class InputSfdcEventLogFiles < InputPlugin
    Plugin.register_input('sfdc_event_log_files', self)

    class << self
      def transaction(config, &control)
        oauth = config.param('oauth', :hash)
        task = {
          'login_url' => config.param('login_url', :string, default: 'https://login.salesforce.com'),
          'oauth_client_id' => oauth['id'],
          'oauth_client_secret' => oauth['secret'],
          'username' => config.param('username', :string),
          'password' => config.param('password', :string),
          'last_log_date' => config.param('last_log_date', :string, default: '0001-01-01T00:00:00Z'),
          'max_retry_times' => config.param('max_retry_times', :integer, default: 2),
        }
        idx = -1
        schema = config.param('schema', :array).map { |c| idx += 1; Column.new(idx, c['name'], c['type'].to_sym) }
        threads = config.param('threads', :integer, default: 2)
        task['client'] = client = HTTPClient.new

        begin
          oauth(task)
          task['records'] = query(task)

          reports = yield(task, schema, threads)

          last_log_date_report = reports.max_by { |report|
            report['last_log_date']
          }
          config.merge(last_log_date_report)
        rescue
          # TODO: log
          raise
        end
      end

    private

      def oauth(task)
        client = task['client']
        params = {
          :grant_type => 'password',
          :client_id => task['oauth_client_id'],
          :client_secret => task['oauth_client_secret'],
          :username => task['username'],
          :password => task['password']
        }
        with_retry(task) {
          res = client.post(task['login_url'] + '/services/oauth2/token', params, :Accept => 'application/json; charset=UTF-8')
          parsed = JSON.parse(res.body)
          client.base_url = parsed['instance_url']
          client.default_header = { 'Authorization' => 'Bearer ' + parsed['access_token'] }
          nil
        }
      end

      def query(task)
        client = task['client']
        query = "Select LogDate, EventType, LogFile from EventLogFile Where LogDate > #{task['last_log_date']}"
        with_retry(task) {
          res = client.get('/services/data/v32.0/query/', :q => query)
          JSON.parse(res.body)['records']
        }
      end

      def with_retry(task)
        retry_times = 0
        begin
          yield
        rescue
          # TODO: log
          retry_times += 1
          retry if retry_times < task['max_retry_times']
          raise
        end
      end
    end

    def initialize(task, schema, index, page_builder)
      @schema = schema
      @page_builder = page_builder
      @records = task['records']
      @last_log_date = Time.parse(task['last_log_date'])
      @client = task['client']
    end

    def run
      columns = @schema.map { |c| c.name }
      @page_builder.finish
      @records.each do |record|
        event_type = record['EventType']
        @last_log_date = [@last_log_date, Time.parse(record['LogDate']).to_i].max
        log_file = record['LogFile']
        CSV.parse(@client.get_content(log_file), headers: true) do |row|
          row['TIMESTAMP'] = Time.parse(row['TIMESTAMP']).to_i
          @page_builder.add(row.to_hash.values_at(*columns))
        end
      end
      @page_builder.finish unless @records.empty?

      commit_report = {
        'last_log_date' => @last_log_date.xmlschema
      }
      commit_report
    end
  end

end
