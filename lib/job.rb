class Job < ActiveRecord::Base

  belongs_to :ontology

  attr_accessor :geo_type, :fields

  class << self
    def available(*hash)
      hash = hash.any? ? hash.first : {}
      options = {:crashed_at => 5.minutes.ago.to_f}.merge!(hash)
      sql = "(worker_key IS NULL AND finished_at IS NULL)"
      sql << " OR (worker_key IS NOT NULL AND ((started_at IS NULL) OR (started_at < ?)))"
      sql << " OR (worker_key IS NULL AND finished_at IS NOT NULL AND ((working_at IS NULL) OR (started_at IS NULL)))"
      if options[:count]
        Job.count(:conditions => [sql, options[:crashed_at]])
      else
        Job.first(:conditions => [sql, options[:crashed_at]])
      end
    end

    def load_item(key)
      case key
        when /^GSM/
          m = Sample
        when /^GSE/
          m = SeriesItem
        when /^GPL/
          m = Gminer::Platform
        when /^GDS/
          m = Dataset
      end
      m.first(:conditions => {:geo_accession => key})
    end
  end

  def has_blank_field
    now = Time.now.to_f
    update_attributes(:started_at => now, :working_at => now, :finished_at => now)
  end

  def started(worker_key)
    update_attributes(:worker_key => worker_key, :started_at => Time.now.to_f)
  end

  def working
    update_attributes(:working_at => Time.now.to_f)
  end

  def finished
    update_attributes(:finished_at => Time.now.to_f, :worker_key => nil)
  end

  def failed
    update_attributes(:started_at => nil, :finished_at => nil, :working_at => nil, :worker_key => nil)
  end
end
