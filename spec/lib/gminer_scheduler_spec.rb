require File.dirname(__FILE__) + '/../spec_helper'

describe GminerScheduler do

  before(:each) do
    @mq = mock("message_queue")
    @worker_max = 5
    @s = GminerScheduler.new(@worker_max, @mq)
  end

  describe "create_worker" do
    it "should create a worker and send a prepare message" do
      Worker.should_receive(:create).with({:worker_key=>"1234", :working=>false}).and_return("worker")
      @s.should_receive(:publish).with("worker-1234", "{\"command\":\"prepare\"}").and_return(true)
      @s.create_worker("1234").should be_true
    end
  end

  describe "send_job" do
    it "should set the worker working and send a job message" do
      worker = Worker.new
      Worker.should_receive(:working).with("1234").and_return(true)
      @s.should_receive(:publish).with("worker-1234", "{\"command\":\"job\",\"key\":\"value\"}").and_return(true)
      @s.send_job("1234", {'key' => 'value'}).should be_true
    end
  end

  describe "process" do
    before(:each) do
      @message = {'worker_key' => '1234', 'job_id' => '12'}
    end

    it "should create the worker" do
      @message.merge!({'command' => 'alive'})
      JSON.should_receive(:parse).with(@message).and_return(@message)
      @s.should_receive(:create_worker).with("1234").and_return(true)
      @s.process(@message)
    end

    it "should start the job" do
      @message.merge!({'command' => 'ready'})
      JSON.should_receive(:parse).with(@message).and_return(@message)
      @s.should_receive(:start_job).with("1234").and_return(true)
      @s.process(@message)
    end

    it "should update the job" do
      @message.merge!({'command' => 'working'})
      JSON.should_receive(:parse).with(@message).and_return(@message)
      @s.should_receive(:started_job).with("1234", "12").and_return(true)
      @s.process(@message)
    end

    it "should finish the job" do
      @message.merge!({'command' => 'finished'})
      JSON.should_receive(:parse).with(@message).and_return(@message)
      @s.should_receive(:finished_job).with("1234", "12").and_return(true)
      @s.process(@message)
    end

    it "should fail the job" do
      @message.merge!({'command' => 'failed'})
      JSON.should_receive(:parse).with(@message).and_return(@message)
      @s.should_receive(:failed_job).with("1234", "12").and_return(true)
      @s.process(@message)
    end
  end

  describe "started_job" do
    it "should update the job attributes" do
      job = Job.new
      Job.should_receive(:find).with("12").and_return(job)
      job.should_receive(:started).with("1234").and_return(true)
      @s.started_job("1234", "12")
    end
  end

  describe "finished_job" do
    it "should update the job attributes" do
      job = Job.new
      Job.should_receive(:find).with("12").and_return(job)
      job.should_receive(:finished).and_return(true)
      worker = Worker.new
      Worker.should_receive(:free).with("1234")
      @s.should_receive(:publish).with("worker-1234", "{\"command\":\"prepare\"}").and_return(true)
      @s.finished_job("1234", "12")
    end
  end

  describe "failed_job" do
    it "should update the job attributes" do
      job = Job.new
      Job.should_receive(:find).with("12").and_return(job)
      job.should_receive(:failed).and_return(true)
      worker = Worker.new
      Worker.should_receive(:free).with("1234")
      @s.should_receive(:publish).with("worker-1234", "{\"command\":\"prepare\"}").and_return(true)
      @s.failed_job("1234", "12")
    end
  end

  describe "launch timer" do
    before(:each) do
      EM.should_receive(:add_periodic_timer).with(15).and_yield("timer")
    end

    describe "if needed" do
      it "should launch more" do
        Worker.should_receive(:count).and_return(5)
        Job.should_receive(:available).with(:count => true).and_return(5)
        @s.should_receive(:worker_max).and_return(10)
        @s.should_receive(:publish).with(GminerScheduler::NODE_QUEUE_NAME, {'command' => 'launch'}.to_json).and_return(true)
        @s.launch_timer
      end
    end

    describe "not needed" do
      it "should not launch more if at worker max" do
        Worker.should_receive(:count).and_return(5)
        @s.should_receive(:worker_max).and_return(5)
        @s.should_not_receive(:publish)
        @s.launch_timer
      end

      it "should not launch more if no more jobs" do
        Worker.should_receive(:count).and_return(2)
        Job.should_receive(:available).with(:count => true).and_return(0)
        @s.should_receive(:worker_max).and_return(5)
        @s.should_not_receive(:publish)
        @s.launch_timer
      end
    end
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

  describe "start job" do
    describe "with an available job" do
      before(:each) do
        @ontology = Ontology.new
        @job = Job.new(:ontology => @ontology)
        Job.should_receive(:available).and_return(@job)
      end

      describe "without an existing annotation" do
        describe "with blank field value" do
          it "should not send the job request and mark it finished" do
            @platform = Platform.new
            @platform.should_receive(:description).and_return("")
            @job.should_receive(:field).and_return("description")
            @job.should_receive(:geo_accession).and_return("GPL1355")
            Job.should_receive(:load_item).with("GPL1355").and_return(@platform)
            @job.should_receive(:finished).and_return(true)
            Worker.should_receive(:free).with("1234").and_return(true)
            @s.should_receive(:publish).with("worker-1234", "{\"command\":\"prepare\"}").and_return(true)
            @s.start_job("1234")
          end
        end

        describe "without blank field value" do
          before(:each) do
            @job.stub!(:field).and_return("description")
            @job.stub!(:geo_accession).and_return("GPL1355")
            @job.should_receive(:started).with("1234").and_return(true)
            @platform = Platform.new
            @platform.should_receive(:description).twice.and_return("platform")
            @platform.should_receive(:descriptive_text).and_return("Platform Title")
            Job.should_receive(:load_item).with("GPL1355").and_return(@platform)
            @ontology = Ontology.new
            @ontology.should_receive(:ncbo_id).and_return("1000")
            @ontology.should_receive(:current_ncbo_id).and_return("13578")
            @job.stub!(:ontology).and_return(@ontology)
          end

          describe "with empty stopwords" do
            it "should process the job" do
              @ontology.should_receive(:stopwords).and_return("")
              @s.should_receive(:send_job).with("1234", {'stopwords' => Constants::STOPWORDS, "ncbo_id" => "1000", "current_ncbo_id" => "13578", "geo_accession" => "GPL1355", "value" => "platform", "field" => "description", "description" => "Platform Title", "job_id" => @job.id}).and_return(true)
              @s.start_job("1234")
            end
          end

          describe "with custom stopwords" do
            it "should process the job" do
              @ontology.should_receive(:stopwords).and_return("stopwords")
              @s.should_receive(:send_job).with("1234", {'stopwords' => Constants::STOPWORDS+'stopwords', "ncbo_id" => "1000", "current_ncbo_id" => "13578", "geo_accession" => "GPL1355", "value" => "platform", "field" => "description", "description" => "Platform Title", "job_id" => @job.id}).and_return(true)
              @s.start_job("1234")
            end
          end
        end
      end
    end

    describe "without an available job" do
      it "should call no jobs" do
        Job.should_receive(:available).and_return(nil)
        @s.should_receive(:no_jobs).with("1234").and_return(true)
        @s.start_job("1234")
      end
    end
  end

  describe "no_jobs" do
    it "should delete the worker" do
      @s.should_receive(:publish).with("worker-1234", "{\"command\":\"shutdown\"}").and_return(true)
      worker = Worker.new
      Worker.should_receive(:first).with(:conditions => {:worker_key=> "1234"}).and_return(worker)
      worker.should_receive(:destroy).and_return(true)
      @s.no_jobs("1234")
    end
  end

end
