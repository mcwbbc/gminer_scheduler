DAEMON_ENV = 'test' unless defined?( DAEMON_ENV )

begin
  require 'spec'
rescue LoadError
  require 'rubygems'
  gem 'rspec'
  require 'spec'
end

require File.dirname(__FILE__) + '/../config/environment'
DaemonKit::Application.running!

Spec::Runner.configure do |config|
  # == Mock Framework
  #
  # RSpec uses it's own mocking framework by default. If you prefer to
  # use mocha, flexmock or RR, uncomment the appropriate line:
  #
  # config.mock_with :mocha
  # config.mock_with :flexmock
  # config.mock_with :rr
end

def clean
  ActiveRecord::Base.connection.execute("SET FOREIGN_KEY_CHECKS = 0;")
  (ActiveRecord::Base.connection.tables - %w{schema_migrations}).each do |table_name|
    ActiveRecord::Base.connection.execute("TRUNCATE TABLE #{table_name};")
  end
  ActiveRecord::Base.connection.execute("SET FOREIGN_KEY_CHECKS = 1;")
end