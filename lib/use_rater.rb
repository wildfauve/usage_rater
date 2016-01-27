class UseRater
=begin
{
    "id": "f7ded8d1-32fd-46a4-bfb4-e5539b0448e5",
    "kind": "telemetry_measurements",
    "measurements": [
        {
            "ended_at": "2015-09-11 12:29:59 UTC",
            "started_at": "2015-09-11 12:00:00 UTC",
            "state": "final",
            "unit_code": "KWH",
            "value": 0.1
        },
        {
            "ended_at": "2015-09-11 12:59:59 UTC",
            "started_at": "2015-09-11 12:30:00 UTC",
            "state": "final",
            "unit_code": "KWH",
            "value": 0.0
        },
    ],
    "telemetry_device_id": "5c74fbe4-2084-4339-a3ef-23f8d4d7de55"
}
=end
#  telemetry_id, channel_op_code, supply_node_id, reading_time, read, state
  def process(plan, message)
    device_id = message["telemetry_device_id"]
    channel_id = message["id"]
    message["measurements"].each do |reading|
      puts "===> Device #{device_id} Channel: #{channel_id} Read Time: #{reading["started_at"]},  Use: #{reading["value"]}"
      model = UseRaterModel.new.create_or_update(device_id, channel_id, reading, plan.rate(reading), plan.symbol, plan.charge_type)
      event = build_charge_event(model, plan)
      KafkaAdapter.new.send(event)
    end

  end

  def build_charge_event(model, plan)
    model.new? ? model.to_hash.merge(headers(plan)) : model.to_hash.merge(headers(plan)).merge(determine_op(model))
  end

  def headers(plan)
    {
      kind: "rated_use_charge",
      plan: plan.symbol,
      charge_type: plan.charge_type
    }

  end

  def determine_op(model)
    diff = model.charge - model.find_state["charge"]
    case
    when diff >= 0
      {op: :inc, op_value: diff}
    else
      {op: :dec, op_value: diff}
    end
  end

end
