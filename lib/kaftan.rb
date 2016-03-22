module Kaftan

  module ClassMethods
    def prepare(name, statement)
      @statements ||= []
      #@statements[name] = {}
      if statement.class != Kaftan::Statement
        statement = Kaftan::Statement.new(name, nil, @table, statement).build
        #prepare_string = statement
      end
      #@statements[name][:prepare] = @session.prepare(prepare_string)
      @statements << statement.prepare_for_execution(@session)
    end

    def table(name, *args)
      ks = args.find {|h| h.has_key? :in_keyspace}[:in_keyspace]
      raise if ks.nil?
      @session = $cluster.connect(ks.to_s)
      @table = name
    end

    def statement(name)
      @statements.find {|s| s.type == name}
    end

    def field(field_name, type)
      @fields ||= {}
      @fields[field_name] = type
      define_method(field_name) do
        instance_variable_get("@#{field_name}")
      end
      define_method("#{field_name}=") do |val|
        convert_val = convert_set_val_to_type(type, val)
#        if type[:type] == :set
#          inst_val = instance_variable_get("@#{field_name}")
#          binding.pry
#          inst_val.class == Set ? set_val = inst_val.merge(convert_val) : set_val = convert_val
#        else
        set_val = convert_val
#        end
        instance_variable_set("@#{field_name}", set_val)
        convert_val
      end
    end

    def after_save(method)
      @after_callbacks ||= {}
      @after_callbacks.has_key?(:save) ? @after_callbacks[:save] << method :  @after_callbacks[:save] = [method]
    end

    def fields
      @fields
    end

    def session
      @session
    end

    def after_callbacks(type)
      if @after_callbacks
        @after_callbacks[type]
      else
        []
      end
    end

    # for custom defined prepares at the Class level
    def execute(name)
      statement = self.statement(name)[:statement]
      self.session(statement.prepare_string)
    end


    def find(args)
      #puts "===> find"
      statement = self.statement(__callee__)
      find = $session.execute(statement.execution, arguments: key_model_to_arguments_array(args, statement))
      raise if find.rows.size > 1
      find.rows.size == 1 ? self.new.init(find.rows.first) : self.new
    end


    def build_default_prepares
      self.prepare(:update, Kaftan::Statement.new(:update, self.fields, @table).build)
      self.prepare(:insert, Kaftan::Statement.new(:insert, self.fields, @table).build)
      self.prepare(:find, Kaftan::Statement.new(:find, self.fields, @table).build)
    end

    def key_model_to_arguments_array(params, statement)
      statement.key_fields.inject([]) {|args, field| args << params[field]; args}
    end

  end # Classmethods

  def self.included(base)
    base.extend(ClassMethods)
  end


  def init(db_data)
    self.class.fields.keys.each {|field| self.send("#{field}=", db_data[field.to_s]) if db_data[field.to_s]}
    @db_init = true
    self
  end

  def insert
    #puts "===> create"
    statement = self.class.statement(__callee__)
    $session.execute(statement.execution, arguments: model_to_arguments_hash(statement))
    execute_callbacks(:after, :save)
  end

  def update
    #puts "===> update"
    statement = self.class.statement(__callee__)
    $session.execute(statement.execution, arguments: model_to_arguments_array(statement))
    execute_callbacks(:after, :save)
  end

  def find
    #puts "===> find"
    statement = self.class.statement(__callee__)
    $session.execute(statement.execute, arguments: model_to_arguments_array(statement))
  end


  def execute_callbacks(location, type)
    self.class.after_callbacks(type).each {|method| self.send(method)}
  end

  def new?
    @db_init ? false : true
  end

  def model_to_arguments_hash(statement)
    statement.all_fields.inject({}) {|args, field| args[field] = self.send(field); args}
  end

  def model_to_arguments_array(statement)
    statement.variable_fields.inject([]) {|args, field| args << self.send(field); args}
                              .concat(statement.key_fields.inject([]) {|args, field| args << self.send(field); args})
  end


  def convert_set_val_to_type(type, val)
    #puts "type: #{type}, val: #{val}"
    if type[:with]
      self.send(type[:with], val)
    else
      coerse_type(type, val)
    end
  end

  def coerse_type(type, val)
    case type[:type]
    when :text
      if val == String
        val
      else
        val.to_s
      end
    when :decimal
      if val.class == BigDecimal
        val
      elsif val.class == Integer
        BigDecimal.new(val, 6)
      elsif val.class == Float
        BigDecimal.new(val, 6)
      else
        binding.pry
      end
    when :time
      if val.class == Time
        val
      else
        Time.parse(val)
      end
    when :set
      if val.class == Set
        val
      else
        Set.new([val])
      end
    when :map
      val
    else
      raise ArgumentError
    end
  end

  def day_from_time(time)
    Time.new(time.year, time.month, time.day, 0,0,0,"+00:00")
  end


end
