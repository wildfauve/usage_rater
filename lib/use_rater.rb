class UseRater

  Struct.new("Telemetry", :kind, :telemetry_device_id, :equipment_id, :measurements, :day)
  Struct.new("Measurement", :started_at, :ended_at, :value, :unit_code, :state, :day, :op)


  class Error < StandardError ; end
  class InvalidIdentifiers < Error ; end
=begin
{"kind"=>"telemetry_measurements",
 "id"=>"eb78e8f1-4be1-46bd-b91e-db65abb52427",
 "equipment_id"=>"1ac36acc-8046-490c-8c1f-ebb2fb44bad8",
 "telemetry_device_id"=>"6686a68a-ca21-4d6c-bb0f-647d1c402b9a",
 "measurements"=>
  [{"started_at"=>"2015-09-10T12:00:00Z",
    "ended_at"=>"2015-09-10T12:29:59Z",
    "value"=>0.1,
    "unit_code"=>"KWH",
    "state"=>"final",
    "op"=>{"op_code"=>"inc", "op_value"=>"0.0"}},
=end
#  telemetry_id, channel_op_code, supply_node_id, reading_time, read, state
  def process(plan, message)
    to_telemetry_per_day(message).each {|telemetry_day| build_changes(telemetry_day, plan)}
  end

  def build_changes(telemetry_day, plan)
    if telemetry_day.equipment_id.nil?
      puts "no equipment_id"
      return
    end
    telemetry_day.measurements.each do |measurement|
      find_or_update(measurement, telemetry_day.telemetry_device_id, telemetry_day.equipment_id, plan)
    end

  end

  def find_or_update(measurement, telemetry_device_id, equipment_id, plan)
    model = UseRaterModel.find({telemetry_device_id: telemetry_device_id, equipment_id: equipment_id, started_at: measurement.started_at})
    if model.new?
      init_params(model, measurement, telemetry_device_id, equipment_id, plan)
      binding.pry
      model.insert
    else
      update_measurement(model, measurement, telemetry_device_id, equipment_id, plan)
    end
  end

  def init_params(model, measurement, telemetry_device_id, equipment_id, plan)
    model.equipment_id = equipment_id
    model.telemetry_device_id = telemetry_device_id
    model.started_at = measurement.started_at
    model.ended_at = measurement.ended_at
    model.charge_events = {}
    model.charge_events[Time.now.utc.iso8601] = create_versions(plan, measurement)
    model
  end

  def create_versions(plan, measurement)
    charges = plan.rate(measurement)
    Cassandra::UDT.new(
        {
          rated_usage: measurement[:value],
          charge: BigDecimal(charges.charge,3),
          charge_type: charges.charge_type.to_s,
          plan_symbol: charges.symbol.to_s
        }
    )
  end

  def update_params(model, measurement, plan)
    binding.pry
    measurement.op["op_code"] == "inc" ? op = :+ : op = :-
    charges = plan.rate(measurement)
    model.read = measurement.value
    model.charge = charges.charge
    model.charge_type = charges.charge_type
    model.plan_symbol = charges.symbol
  end

  def update_measurement(model, measurement, telemetry_device_id, equipment_id, plan)
    # Work out whether we have already rated this particular update
    # This is where the model and message values are the same
    if measurement.value != model.read # we haven't processed the message
      update_params(model, measurement, plan)
    end
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
    read_diff = model.read - model.find_state["read"]
    case
    when diff >= 0
      {op: :inc, op_value: diff, read_op: :inc, read_op_value: read_dif}
    else
      {op: :dec, op_value: diff, read_op: :dec, read_op_value: read_dif}
    end
  end

  def to_telemetry_per_day(message)
    per_day_measures = per_day(message["measurements"].collect {|m| to_measure(m)})
    per_day_measures.inject([]) do |tels, m_day|
      t = Struct::Telemetry.new
      t.kind = message["kind"]
      t.telemetry_device_id = message["telemetry_device_id"]
      t.equipment_id = message["equipment_id"]
      t.measurements = m_day
      t.day = t.measurements.first.day
      tels << t
    end
  end

  def to_measure(measure)
    m = Struct::Measurement.new
    m.started_at = measure["started_at"]
    m.ended_at = measure["ended_at"]
    m.value = BigDecimal(measure["value"],3)
    m.unit_code = measure["unit_code"]
    m.state = measure["state"]
    m.day = day_from_time(m.started_at)
    m.op = measure["op"]
    m
  end

  def per_day(measures)
    days = measures.map(&:day).uniq!
    days.inject([]) {|i, day| i << measures.select {|d| d.day == day}; i}
  end

  def day_from_time(time)
    #Time.new(time.year, time.month, time.day, 0,0,0,"+00:00").utc
    time[0..9]
  end


end
