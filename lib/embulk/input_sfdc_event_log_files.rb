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
        threads = config.param('threads', :integer, default: 2)
        idx = -1
        schema = config.param('schema', :array, default: []).map { |c| idx += 1; Column.new(idx, c['name'], c['type'].to_sym) }

        begin
          client = HTTPClient.new
          parsed = oauth(client, task)
          task['instance_url'] = parsed['instance_url']
          task['access_token'] = parsed['access_token']
          client.base_url = task['instance_url']
          client.default_header = { 'Authorization' => 'Bearer ' + task['access_token'] }

          task['records'] = query(client, task)

          if schema.empty?
            raise 'empty schema given'
          end
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

      def oauth(client, task)
        params = {
          :grant_type => 'password',
          :client_id => task['oauth_client_id'],
          :client_secret => task['oauth_client_secret'],
          :username => task['username'],
          :password => task['password']
        }
        with_retry(task) {
          res = client.post(task['login_url'] + '/services/oauth2/token', params, :Accept => 'application/json; charset=UTF-8')
          JSON.parse(res.body)
        }
      end

      def query(client, task)
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

    attr_reader :task
    attr_reader :schema
    attr_reader :page_builder

    def run
      client = HTTPClient.new
      client.base_url = task['instance_url']
      client.default_header = { 'Authorization' => 'Bearer ' + task['access_token'] }
      records = task['records']
      last_log_date = Time.parse(task['last_log_date'])
      columns = schema.map { |c| c.name }
      records.each do |record|
        event_type = record['EventType']
        last_log_date = [last_log_date, Time.parse(record['LogDate']).to_i].max
        log_file = record['LogFile']
        log_body = client.get_content(log_file)
        CSV.parse(log_body, headers: true) do |row|
          if row['TIMESTAMP']
            begin
              row['TIMESTAMP'] = Time.parse(row['TIMESTAMP']).to_i
            rescue
              # ignore
            end
          end
          page_builder.add(row.to_hash.values_at(*columns).map(&:to_s))
        end
      end
      page_builder.finish unless records.empty?
      commit_report = {
        'last_log_date' => last_log_date.xmlschema
      }
      commit_report
    end
  end

end
