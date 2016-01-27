class UseRaterModel

  attr_accessor :find_state

  class << self
    def prepare(name, statement)
      @statements ||= {}
      @statements[name] = $session.prepare(statement)
    end

    def statement(name)
      @statements[name]
    end
  end

  prepare :find_charge, "SELECT * from rated_use WHERE telemetry_device_id = ? AND telemetry_channel_id = ? AND started_at = ?"
  prepare :insert_charge, "INSERT INTO rated_use (telemetry_device_id, telemetry_channel_id, started_at, ended_at, read, charge, charge_type, plan_symbol)" \
                            "VALUES (:telemetry_device_id, :telemetry_channel_id, :started_at, :ended_at, :read, :charge, :charge_type, :plan_symbol)"
  prepare :update_charge, "UPDATE rated_use SET read = ?, charge = ? WHERE telemetry_device_id = ? AND telemetry_channel_id = ? AND started_at = ?"





=begin

CREATE KEYSPACE Rating
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TYPE reading (
      time timestamp,
      read decimal,
      state text
  );

CREATE TABLE rated_use (
  telemetry_device_id text,
  telemetry_channel_id text,
  started_at timestamp,
  ended_at timestamp,
  read decimal,
  charge decimal,
  charge_type text,
  plan_symbol text,
  PRIMARY KEY (telemetry_device_id, telemetry_channel_id, started_at)
);
=end

  def initialize
  end


  def create(model)
    puts "===> create"
    $session.execute(UseRaterModel.statement(:insert_charge), arguments: model)
  end

  def update(model)
    result = $session.execute(self.class.statement(:update_charge),
                                arguments: [
                                  model[:read],
                                  model[:charge],
                                  model[:telemetry_device_id],
                                  model[:telemetry_channel_id],
                                  model[:started_at]
                                ])
  end

  def create_or_update(telemetry_device_id, telemetry_channel_id, reading, charge, plan_symbol, plan_charge_type)
    @model = map_types(telemetry_device_id, telemetry_channel_id, reading, charge, plan_symbol, plan_charge_type)
    rate = find(@model)
    rate ? update(@model) : create(@model)
    self
  end

  def find(model)
    result = $session.execute(UseRaterModel.statement(:find_charge),
                                arguments: [
                                  model[:telemetry_device_id],
                                  model[:telemetry_channel_id],
                                  model[:started_at]
                                ])
    raise if result.rows.size > 1
    @find_state = result.rows.first
    #result.rows.size == 1 ? result.rows.first : nil
  end

  def map_types(telemetry_device_id, telemetry_channel_id, reading, charge, plan_symbol, plan_charge_type)
    {
      telemetry_device_id: telemetry_device_id,
      telemetry_channel_id: telemetry_channel_id,
      ended_at: to_time(reading["ended_at"]),
      started_at: to_time(reading["started_at"]),
      #unit_code: reading["unit_code"],
      read: to_dec(reading["value"]),
      charge: charge.value,
      charge_type: plan_charge_type.to_s,
      plan_symbol: plan_symbol.to_s
    }
  end

  def telemetry_device_id
    self.map(__callee__)
  end

  def telemetry_channel_id
    self.map(__callee__)
  end

  def ended_at
    self.map(__callee__)
  end

  def started_at
    self.map(__callee__)
  end

  def read
    self.map(__callee__)
  end

  def charge
    self.map(__callee__)
  end

  def map(method)
    @model[method]
  end

  def to_hash
    @model
  end

  def new?
    @find_state.nil?
  end

  def to_time(t)
    Time.parse(t)
  end

  def to_dec(n)
    BigDecimal.new(n,4)
  end


end
