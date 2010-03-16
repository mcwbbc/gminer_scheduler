# Your starting point for daemon specific classes. This directory is
# already included in your load path, so no need to specify it.
class GminerScheduler

  SCHEDULER_QUEUE_NAME = 'gminer-scheduler'
  NODE_QUEUE_NAME = 'gminer-node'

  attr_accessor :worker_max, :mq, :listen_queue

  def initialize(worker_max, mq)
    @worker_max = worker_max.to_i
    @mq = mq
    @listen_queue = mq.queue(GminerScheduler::SCHEDULER_QUEUE_NAME, :durable => true)
  end

  def publish(name, msg)
    mq.queue(name).publish(msg, :persistent => true)
#    DaemonKit.logger.debug("SENT: #{msg} to #{name}")
  end

  def process(msg)
    message = JSON.parse(msg)
    case message['command']
      when 'alive'
        create_worker(message['worker_key'])
      when 'ready'
        start_job(message['worker_key'])
      when 'working'
        working_job(message['worker_key'], message['job_id'])
      when 'finished'
        finished_job(message['worker_key'], message['job_id'])
      when 'failed'
        failed_job(message['worker_key'], message['job_id'])
      when 'status'
        status_worker(message['worker_key'], message['processing'])
    end # of case
  end

  def create_worker(worker_key)
    w = Worker.create(:worker_key => worker_key, :working => false, :ready => true)
    publish(worker_key, {'command' => 'prepare'}.to_json)
  end

  def status_worker(worker_key, processing)
    if processing && (w = get_worker(worker_key))
      w.working
    else
      free_or_shutdown(worker_key)
    end
  end

  def check_status(worker_key)
#    DaemonKit.logger.debug("worker status: #{worker_key}")
    publish(worker_key, {'command' => 'status'}.to_json)
  end

  def send_job(worker_key, params)
    if w = get_worker(worker_key)
      w.working
      publish(worker_key, params.merge!({'command' => 'job'}).to_json)
    else
      publish(worker_key, {'command' => 'shutdown'}.to_json)
    end
#    DaemonKit.logger.debug("send job: worker:#{worker_key}")
  end

  def start_job(worker_key)
    if w = get_worker(worker_key)
      if job = Job.available
        job.started(worker_key)
        item = Job.load_item(job.geo_accession)
        if !item.send(job.field).blank?
          stopwords = Constants::STOPWORDS+job.ontology.stopwords
          params = {'email' => Constants::EMAIL, 'job_id' => job.id, 'geo_accession' => job.geo_accession, 'field' => job.field, 'value' => item.send(job.field), 'description' => item.descriptive_text, 'ncbo_id' => job.ontology.ncbo_id, 'stopwords' => stopwords}
          send_job(worker_key, params)
          DaemonKit.logger.debug("start job: #{job.id} worker:#{worker_key}")
        else
          job.finished
          free_or_shutdown(worker_key)
        end
      else
        no_jobs(worker_key)
      end
    else
      free_or_shutdown(worker_key)
    end
  end

  def working_job(worker_key, job_id)
    if job = Job.first(:conditions => {:id => job_id})
      job.working
    else
      DaemonKit.logger.debug("ERROR: missing job: #{job_id}")
    end
#    DaemonKit.logger.debug("working job: #{worker_key} -- #{job_id}")
  end

  def failed_job(worker_key, job_id)
    if job = Job.first(:conditions => {:id => job_id})
      job.failed
    else
      DaemonKit.logger.debug("ERROR: missing job: #{job_id}")
    end
    free_or_shutdown(worker_key)
#    DaemonKit.logger.debug("failed job: #{worker_key} -- #{job_id}")
  end

  def finished_job(worker_key, job_id)
    if job = Job.first(:conditions => {:id => job_id})
      job.finished
    else
      DaemonKit.logger.debug("ERROR: missing job: #{job_id}")
    end
    free_or_shutdown(worker_key)
    DaemonKit.logger.debug("finished job: #{worker_key} -- #{job_id}")
  end

  def launch_timer
    DaemonKit.logger.debug("running launch timer")
    EM.add_periodic_timer(10) do
      worker_status_check
      worker_count = Worker.count
      job_count = Job.available(:count => true)
      DaemonKit.logger.debug("Workers: #{worker_count}")
      DaemonKit.logger.debug("jobs: #{job_count}")
      launch_it = (worker_count < worker_max && job_count > 0)
      publish(GminerScheduler::NODE_QUEUE_NAME, {'command' => 'launch'}.to_json) if launch_it
    end
  end

  def worker_status_check
    Worker.all.each do |w|
      if w.crashed?
        w.destroy
      end
    end
  end

  def no_jobs(worker_key)
    if w = get_worker(worker_key)
      w.destroy
      DaemonKit.logger.debug("no jobs destroyed: #{worker_key}")
    end
    publish(worker_key, {'command' => 'shutdown'}.to_json)
  end

  def free_or_shutdown(worker_key)
    if w = get_worker(worker_key)
      w.free
      publish(worker_key, {'command' => 'prepare'}.to_json)
    else
      publish(worker_key, {'command' => 'shutdown'}.to_json)
    end
  end

  def get_worker(worker_key)
    Worker.first(:conditions => {:worker_key => worker_key})
  end

end