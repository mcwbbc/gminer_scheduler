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

    def free(worker_key)
      Worker.update_all({:working => false}, :worker_key => worker_key)
    end

    def working(worker_key)
      Worker.update_all({:working => true}, :worker_key => worker_key)
    end
  end

end