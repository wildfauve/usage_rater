class KafkaPort < AnyPort::Port

  def initialize
    @topic = "rated_use"
  end

  def publish(msg)
    circuit(log: AnyPort::CircuitLog.new(service_address: @topic), circuit_config: {retry: true}) do
      send_to_port(service_address: @topic, port: {on: :kafka}, body: msg)
    end
  end

end
