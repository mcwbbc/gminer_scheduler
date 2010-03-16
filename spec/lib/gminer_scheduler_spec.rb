require File.dirname(__FILE__) + '/../spec_helper'

describe GminerScheduler do

  before(:each) do
    clean
    @worker_max = 5
    @queue = mock("queue")
    @lq = mock("listen_queue")
    @queue.stub!(:bind).and_return(@lq)
    @mq = mock("message_queue")
    @mq.stub!(:queue).and_return(@queue)
    @s = GminerScheduler.new(@worker_max, @mq)
  end

  after(:each) do
    clean
  end

  describe "publish" do
    it "should publish a message to a queue" do
      queue = mock("queue")
      @mq.should_receive(:queue).with("xqueue", {:durable=>true}).and_return(queue)
      queue.should_receive(:publish).with("message", :persistent => true).and_return(true)
      @s.publish("xqueue", "message")
    end
  end

  describe "create_worker" do
    it "should create a worker and send a prepare message" do
      Worker.should_receive(:create).with({:worker_key=>"1234", :working => false, :ready => true}).and_return("worker")
      @s.should_receive(:publish).with("1234", "{\"command\":\"prepare\"}").and_return(true)
      @s.create_worker("1234").should be_true
    end
  end

  describe "send_job" do
    it "should set the worker working and send a job message" do
      worker = Worker.new
      @s.should_receive(:get_worker).with("1234").and_return(worker)
      worker.should_receive(:working).and_return(true)
      @s.should_receive(:publish).with("1234", "{\"command\":\"job\",\"key\":\"value\"}").and_return(true)
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

    it "should have the worker's status" do
      @message.merge!({'command' => 'status', 'processing' => true})
      JSON.should_receive(:parse).with(@message).and_return(@message)
      @s.should_receive(:status_worker).with("1234", true).and_return(true)
      @s.process(@message)
    end

    it "should start the job" do
      @message.merge!({'command' => 'ready'})
      JSON.should_receive(:parse).with(@message).and_return(@message)
      @s.should_receive(:start_job).with("1234").and_return(true)
      @s.process(@message)
    end

    it "should update the job to working" do
      @message.merge!({'command' => 'working'})
      JSON.should_receive(:parse).with(@message).and_return(@message)
      @s.should_receive(:working_job).with("1234", "12").and_return(true)
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

  describe "check_status" do
    it "should send a message to check worker status" do
      @s.should_receive(:publish).with("1234", "{\"command\":\"status\"}").and_return(true)
      @s.check_status("1234").should be_true
    end
  end

  describe "status worker" do
    it "update the workers status to working" do
      worker = Worker.new
      @s.should_receive(:get_worker).with("1234").and_return(worker)
      worker.should_receive(:working).and_return(true)
      @s.status_worker("1234", true)
    end

    it "update the workers status to free" do
      worker = Worker.new
      @s.should_receive(:get_worker).with("1234").and_return(worker)
      worker.should_receive(:free).and_return(true)
      @s.should_receive(:publish).with("1234", "{\"command\":\"prepare\"}").and_return(true)
      @s.status_worker("1234", false)
    end
  end

  describe "working_job" do
    it "should update the job attributes" do
      job = Job.new
      Job.should_receive(:find).with(:first, {:conditions=>{:id=>"12"}}).and_return(job)
      job.should_receive(:working).and_return(true)
      @s.working_job("1234","12")
    end
  end

  describe "finished_job" do
    it "should update the job attributes" do
      job = Job.new
      Job.should_receive(:find).with(:first, {:conditions=>{:id=>"12"}}).and_return(job)
      job.should_receive(:finished).and_return(true)
      worker = Worker.new
      @s.should_receive(:get_worker).with("1234").and_return(worker)
      worker.should_receive(:free).and_return(true)
      @s.should_receive(:publish).with("1234", "{\"command\":\"prepare\"}").and_return(true)
      @s.finished_job("1234", "12")
    end
  end

  describe "failed_job" do
    it "should update the job attributes" do
      job = Job.new
      Job.should_receive(:find).with(:first, {:conditions=>{:id=>"12"}}).and_return(job)
      job.should_receive(:failed).and_return(true)
      worker = Worker.new
      @s.should_receive(:get_worker).with("1234").and_return(worker)
      worker.should_receive(:free).and_return(true)
      @s.should_receive(:publish).with("1234", "{\"command\":\"prepare\"}").and_return(true)
      @s.failed_job("1234", "12")
    end
  end

  describe "launch timer" do
    before(:each) do
      EM.should_receive(:add_periodic_timer).with(10).and_yield("timer")
    end

    describe "if needed" do
      it "should launch more" do
        Worker.should_receive(:count).and_return(5)
        Job.should_receive(:available).with(:count => true).and_return(5)
        @s.should_receive(:worker_max).and_return(10)
        @s.should_receive(:publish).with(GminerScheduler::NODE_QUEUE_NAME, {'command' => 'launch'}.to_json).and_return(true)
        @s.launch_timer
      end

      it "should launch more for 1 job" do
        Worker.should_receive(:count).and_return(5)
        Job.should_receive(:available).with(:count => true).and_return(1)
        @s.should_receive(:worker_max).and_return(10)
        @s.should_receive(:publish).with(GminerScheduler::NODE_QUEUE_NAME, {'command' => 'launch'}.to_json).and_return(true)
        @s.launch_timer
      end
    end

    describe "not needed" do
      it "should not launch more if at worker max" do
        Worker.should_receive(:count).and_return(5)
        Job.should_receive(:available).with(:count => true).and_return(2)
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

  describe "worker_status_check" do
    describe "with workers and no jobs" do
      before(:each) do
        @worker = Worker.new
        Worker.should_receive(:all).and_return([@worker])
      end

      it "should check status and not destroy" do
        @worker.should_receive(:crashed?).and_return(false)
        @worker.should_not_receive(:destroy)
        @s.worker_status_check
      end

      it "should check status and destroy" do
        @worker.should_receive(:crashed?).and_return(true)
        @worker.should_receive(:destroy).and_return(true)
        @s.worker_status_check
      end
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
            worker = Worker.new
            @s.should_receive(:get_worker).with("1234").and_return(worker)
            @job.should_receive(:started).with("1234").and_return(true)
            
            @platform = Gminer::Platform.new
            @platform.should_receive(:description).and_return("")
            Job.should_receive(:load_item).with("GPL1355").and_return(@platform)
            @job.should_receive(:field).and_return("description")
            @job.should_receive(:geo_accession).and_return("GPL1355")

            @job.should_receive(:finished).and_return(true)
            @s.should_receive(:free_or_shutdown).with("1234").and_return(true)

            @s.start_job("1234")
          end
        end

        describe "without blank field value" do
          before(:each) do
            worker = Worker.new
            @s.should_receive(:get_worker).with("1234").and_return(worker)
            @job.stub!(:id).and_return(12)
            @job.stub!(:field).and_return("description")
            @job.stub!(:geo_accession).and_return("GPL1355")
            @job.should_receive(:started).with("1234").and_return(true)
            @platform = Gminer::Platform.new
            @platform.should_receive(:description).twice.and_return("platform")
            @platform.should_receive(:descriptive_text).and_return("Platform Title")
            Job.should_receive(:load_item).with("GPL1355").and_return(@platform)
            @ontology = Ontology.new
            @ontology.should_receive(:ncbo_id).and_return("1000")
            @job.stub!(:ontology).and_return(@ontology)
          end

          describe "with empty stopwords" do
            it "should process the job" do
              @ontology.should_receive(:stopwords).and_return("")
              @s.should_receive(:send_job).with("1234", {'stopwords' => Constants::STOPWORDS, "ncbo_id" => "1000", "geo_accession" => "GPL1355", "value" => "platform", "field" => "description", "description" => "Platform Title", "job_id" => @job.id, "email"=>"jfgeiger@mcw.edu"}).and_return(true)
              @s.start_job("1234")
            end
          end

          describe "with custom stopwords" do
            it "should process the job" do
              @ontology.should_receive(:stopwords).and_return("stopwords")
              @s.should_receive(:send_job).with("1234", {'stopwords' => Constants::STOPWORDS+'stopwords', "ncbo_id" => "1000", "geo_accession" => "GPL1355", "value" => "platform", "field" => "description", "description" => "Platform Title", "job_id" => @job.id, "email"=>"jfgeiger@mcw.edu"}).and_return(true)
              @s.start_job("1234")
            end
          end
        end
      end
    end

    describe "without an available job" do
      it "should call no jobs" do
        worker = Worker.new
        @s.should_receive(:get_worker).with("1234").and_return(worker)
        Job.should_receive(:available).and_return(nil)
        @s.should_receive(:no_jobs).with("1234").and_return(true)
        @s.start_job("1234")
      end
    end
  end

  describe "no_jobs" do
    it "should delete the worker" do
      @s.should_receive(:publish).with("1234", "{\"command\":\"shutdown\"}").and_return(true)
      worker = Worker.new
      Worker.should_receive(:first).with(:conditions => {:worker_key=> "1234"}).and_return(worker)
      worker.should_receive(:destroy).and_return(true)
      @s.no_jobs("1234")
    end
  end

end
