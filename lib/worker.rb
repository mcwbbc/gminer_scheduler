class Worker < ActiveRecord::Base

  class << self
    def available(*hash)
      hash = hash.any? ? hash.first : {}
      options = {}.merge!(hash)
      if options[:count]
        count(:conditions => {:working => false, :ready => true})
      else
        first(:conditions => {:working => false, :ready => true})
      end
    end
  end

  def free
    self.working = false
    self.save!
  end

  def working
    self.working = true
    self.save!
  end

  def crashed?
    self.updated_at < 2.minutes.ago
  end

end