class StreamConsumer < AnyPort::Port

  def initialize
    @input_topic = "measurements"
    @handler = StreamHandler.new
  end

  def fetch
    puts "===> Starting Fetch Wait Loop"
    circuit(log: AnyPort::CircuitLog.new(service_address: @input_topic)) do
      result = get_from_port(service_address: @input_topic, port: {on: :kafka, group_name: "usage_rater_group"}) do |message, offset|
        puts "OffSet: #{offset}"
        @handler.process message
      end
    end
#    loop do
#      messages = @stream.fetch
#      messages.each do |m|
#        @handler.process m
#      end
#    end
  end

end
