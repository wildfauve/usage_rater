class UseRaterModel

=begin

CREATE KEYSPACE Rating
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };

CREATE TABLE rated_use (
  telemetry_device_id text,
  equipment_id text,
  started_at text,
  ended_at text,
  charge_events map<text, frozen<charge_version>>,
  PRIMARY KEY (equipment_id, telemetry_device_id, started_at)
);

CREATE TYPE rating.charge_version (
      rated_usage decimal,
      charge decimal,
      charge_type text,
      plan_symbol text
  );
=end

  include Kaftan

  table :rated_use, in_keyspace: :rating

  field :equipment_id, type: :text, key: true
  field :telemetry_device_id, type: :text, key: true
  field :started_at, type: :text, key: true
  field :ended_at, type: :text
  field :charge_events, type: :map, member_type: :udt

  build_default_prepares


  def to_hash
    @model
  end


end
