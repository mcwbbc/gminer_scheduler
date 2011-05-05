# This file is copied to spec/ when you run 'rails generate rspec:install'
ENV["DAEMON_ENV"] ||= 'test'
require File.expand_path("../../config/environment", __FILE__)

DaemonKit::Application.running!
RSpec.configure do |config|
  # == Mock Framework
  #
  # If you prefer to use mocha, flexmock or RR, uncomment the appropriate line:
  #
  # config.mock_with :mocha
  # config.mock_with :flexmock
  # config.mock_with :rr
  config.mock_with :rspec

end

def clean
  ActiveRecord::Base.connection.execute("SET FOREIGN_KEY_CHECKS = 0;")
  (ActiveRecord::Base.connection.tables - %w{schema_migrations}).each do |table_name|
    ActiveRecord::Base.connection.execute("TRUNCATE TABLE #{table_name};")
  end
  ActiveRecord::Base.connection.execute("SET FOREIGN_KEY_CHECKS = 1;")
end