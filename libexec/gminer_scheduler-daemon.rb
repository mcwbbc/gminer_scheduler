# Generated amqp daemon

# Do your post daemonization configuration here
# At minimum you need just the first line (without the block), or a lot
# of strange things might start happening...
DaemonKit::Application.running! do |config|
  # Trap signals with blocks or procs
#  config.trap( 'INT' ) do
#    @scheduler.listen_queue.unsubscribe
#  end
  config.trap( 'INT', Proc.new { @scheduler.listen_queue.unsubscribe } )
  config.trap( 'TERM', Proc.new { @scheduler.listen_queue.unsubscribe } )

end

# IMPORTANT CONFIGURATION NOTE
#
# Please review and update 'config/amqp.yml' accordingly or this
# daemon won't work as advertised.

# Run an event-loop for processing
DaemonKit::AMQP.run do
  # Inside this block we're running inside the reactor setup by the
  # amqp gem. Any code in the examples (from the gem) would work just
  # fine here.

  # Uncomment this for connection keep-alive
  AMQP.conn.connection_status do |status|
    DaemonKit.logger.debug("AMQP connection status changed: #{status}")
    if status == :disconnected
      AMQP.conn.reconnect(true)
    end
  end

  @amq = ::MQ.new
  @amq.prefetch(1)
  @workers = DaemonKit::Config.load('workers')
  @worker_max = DaemonKit.arguments.options[:workers] || @workers[:max]

  @scheduler = GminerScheduler.new(@worker_max, @amq)
  @scheduler.launch_timer

  @scheduler.listen_queue.subscribe do |msg|
#    DaemonKit.logger.debug("MSG: #{msg}")
    @scheduler.process(msg)
  end
end
