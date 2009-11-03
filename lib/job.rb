class Job < ActiveRecord::Base

  belongs_to :ontology

  attr_accessor :geo_type, :fields

  class << self
    def available(*hash)
      hash = hash.any? ? hash.first : {}
      options = {:expired_at => 2.weeks.ago, :crashed_at => 5.minutes.ago}.merge!(hash)
      if options[:count]
        Job.count(:conditions => ["(worker_key IS NULL AND (finished_at IS NULL OR finished_at < ?)) OR (worker_key IS NOT NULL AND started_at < ?)", options[:expired_at], options[:crashed_at]])
      else
        Job.first(:conditions => ["(worker_key IS NULL AND (finished_at IS NULL OR finished_at < ?)) OR (worker_key IS NOT NULL AND started_at < ?)", options[:expired_at], options[:crashed_at]])
      end
    end

    def create_for(geo_accession, ontology_id, field)
      if !j = Job.first(:conditions => {:geo_accession => geo_accession, :field => field, :ontology_id => ontology_id})
        Job.create(:geo_accession => geo_accession, :field => field, :ontology_id => ontology_id)
      end
    end

    def load_item(key)
      case key
        when /^GSM/
          m = Sample
        when /^GSE/
          m = SeriesItem
        when /^GPL/
          m = Platform
        when /^GDS/
          m = Dataset
      end
      m.first(:conditions => {:geo_accession => key})
    end
  end

  def started(worker_key)
    self.update_attributes(:worker_key => worker_key, :started_at => Time.now)
  end

  def finished
    self.update_attributes(:finished_at => Time.now, :worker_key => nil)
  end

  def failed
    self.update_attributes(:started_at => nil, :finished_at => nil, :worker_key => nil)
  end
end
