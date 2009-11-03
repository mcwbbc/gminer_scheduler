require 'active_record'
require 'constants'
require 'job'
require 'utilities'
require 'ontology'
require 'worker'
require 'platform'
require 'dataset'
require 'series_item'
require 'sample'

begin
  require 'amqp'
  require 'mq'
rescue LoadError
  $stderr.puts "Missing amqp gem. Please run 'gem install amqp'."
  exit 1
end
