module Kaftan

  class Statement

    attr_accessor :prepare_string, :type, :execution

    def initialize(crud_name, fields, table, prepare_str=nil)
      @type = crud_name
      @fields = fields
      @table = table
      @prepare_string = prepare_str
    end

    def build
      if self.respond_to? @type
        #supported prepare
        self.send(@type)
      else
        raise if @prepare_string.nil?
      end
      self
    end

    def prepare_for_execution(session)
      @execution = session.prepare(@prepare_string)
      self
    end

    def update
      build_update_base
      #build_meta(__callee__)
    end

    def insert
      build_insert_base
      #build_meta(__callee__)
    end

    def find
      build_find_base
    end

    def build_update_base
      @prepare_string = "UPDATE #{@table} SET"
      @prepare_string += variable_fields.inject("") {|str, field| str += " #{field} = ?,"; str }.chop!
      @prepare_string += " WHERE "
      @prepare_string += key_fields_and_constructor
    end

    def build_insert_base
      @prepare_string = "INSERT into #{@table} ("
      @prepare_string += all_fields.inject("") {|str, field| str += "#{field},"}.chop!
      @prepare_string += ") VALUES ("
      @prepare_string += all_fields.inject("") {|str, field| str += ":#{field},"}.chop!
      @prepare_string += ")"
    end

    def build_find_base
      #prepare :find_measures, "SELECT * from telemetry_measures WHERE telemetry_id = ? AND channel_id = ? AND day = ?"
      @prepare_string = "SELECT * from #{@table} WHERE "
      @prepare_string += key_fields_and_constructor
    end

    def key_fields_and_constructor
      key_fields.inject("") {|str, field| str += " #{field} = ? AND"; str }[0..-5]
    end

    def key_fields
      @fields.select {|k,v| v[:key] == true}.keys
    end

    def variable_fields
      @fields.select {|k,v| !v.has_key? :key}.keys
    end

    def all_fields
      @fields.keys
    end

  end
end
