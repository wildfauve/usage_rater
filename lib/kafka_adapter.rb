class KafkaAdapter

  def initialize
    @topic = "rated_use"
  end

  def send(msg)
    message = WaterDrop::Message.new(@topic, JSON.generate(msg))
    message.send!
  end

end
