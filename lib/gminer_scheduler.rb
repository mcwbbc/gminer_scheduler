# Your starting point for daemon specific classes. This directory is
# already included in your load path, so no need to specify it.
class GminerScheduler

  SCHEDULER_QUEUE_NAME = 'gminer-scheduler'
  NODE_QUEUE_NAME = 'gminer-node'

  attr_accessor :worker_max, :mq

  def initialize(worker_max, mq)
    @worker_max = worker_max.to_i
    @mq = mq
  end

  def publish(name, msg)
    mq.queue(name, :durable => true, :auto_delete => !!name.match(/worker-.+/)).publish(msg, :persistent => true)
  end

  def process(msg)
    message = JSON.parse(msg)
    case message['command']
      when 'alive'
        create_worker(message['worker_key'])
      when 'ready'
        start_job(message['worker_key'])
      when 'working'
        started_job(message['worker_key'], message['job_id'])
      when 'finished'
        finished_job(message['worker_key'], message['job_id'])
      when 'failed'
        failed_job(message['worker_key'], message['job_id'])
      when 'shutdown'
        # do nothing, since rabbit will autokill the queue
        # delete("worker-#{message['worker_key']}")
    end # of case
  end

  def create_worker(worker_key)
    w = Worker.create(:worker_key => worker_key, :working => false)
    publish("worker-#{worker_key}", {'command' => 'prepare'}.to_json)
  end

  def send_job(worker_key, params)
    Worker.working(worker_key)
    publish("worker-#{worker_key}", params.merge!({'command' => 'job'}).to_json)
  end

  def start_job(worker_key)
    if job = Job.available
      job.started(worker_key)
      item = Job.load_item(job.geo_accession)
      if !item.send(job.field).blank?
        stopwords = Constants::STOPWORDS+job.ontology.stopwords
        params = {'job_id' => job.id, 'geo_accession' => job.geo_accession, 'field' => job.field, 'value' => item.send(job.field), 'description' => item.descriptive_text, 'ncbo_id' => job.ontology.ncbo_id, 'current_ncbo_id' => job.ontology.current_ncbo_id, 'stopwords' => stopwords}
        send_job(worker_key, params)
        DaemonKit.logger.debug("sent job:#{job.id} worker:#{worker_key}")
      else
        job.finished
        Worker.free(worker_key)
        publish("worker-#{worker_key}", {'command' => 'prepare'}.to_json)
      end
    else
      no_jobs(worker_key)
    end
  end

  def started_job(worker_key, job_id)
    Job.find(job_id).started(worker_key)
  end

  def failed_job(worker_key, job_id)
    Job.find(job_id).failed
    Worker.free(worker_key)
    publish("worker-#{worker_key}", {'command' => 'prepare'}.to_json)
  end

  def finished_job(worker_key, job_id)
    Job.find(job_id).finished
    Worker.free(worker_key)
    publish("worker-#{worker_key}", {'command' => 'prepare'}.to_json)
  end

  def launch_timer
    DaemonKit.logger.debug("running launch timer")
    EM.add_periodic_timer(15) do
      worker_count = Worker.count
      DaemonKit.logger.debug("Workers: #{worker_count}")
      launch_it = (worker_count < worker_max && Job.available(:count => true) > 1)
      publish(GminerScheduler::NODE_QUEUE_NAME, {'command' => 'launch'}.to_json) if launch_it
    end
  end

  def no_jobs(worker_key)
    publish("worker-#{worker_key}", {'command' => 'shutdown'}.to_json)
    w = Worker.first(:conditions => {:worker_key => worker_key})
    w.destroy
  end

end