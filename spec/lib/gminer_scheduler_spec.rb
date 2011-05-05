require 'spec_helper'

describe GminerScheduler do

  before(:each) do
    clean
    @worker_max = 5
    @queue = mock("queue")
    @mq = mock("message_queue")
    @mq.stub!(:queue).and_return(@queue)
    @s = GminerScheduler.new(@worker_max, @mq)
  end

  after(:each) do
    clean
  end

  describe "publish" do
    it "should publish a message to a queue" do
      @queue.should_receive(:publish).with("message", :persistent => true).and_return(true)
      @s.publish("queue", "message")
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
    describe "with worker" do
      it "should set the worker working and send a job message" do
        ontology = Ontology.new(:stopwords => "stop", :expand_ontologies => '1150')
        job = Job.new(:ontology => ontology, :field_name => 'description')
        dataset = Dataset.new(:description => 'desc')
        worker = Worker.new
        @s.should_receive(:get_worker).with("1234").and_return(worker)
        worker.should_receive(:working).and_return(true)
        @s.should_receive(:publish).with("1234", "{\"email\":\"jfgeiger@mcw.edu\",\"job_id\":null,\"geo_accession\":null,\"field\":\"description\",\"value\":\"desc\",\"description\":null,\"ncbo_id\":null,\"ontology_name\":null,\"stopwords\":\"a,about,above,across,after,again,against,all,almost,alone,along,already,also,although,always,among,an,and,another,any,anybody,anyone,anything,anywhere,are,area,areas,around,as,ask,asked,asking,asks,at,away,b,back,backed,backing,backs,be,became,because,become,becomes,been,before,began,behind,being,beings,best,better,between,big,both,but,by,c,came,can,cannot,case,cases,certain,certainly,clear,clearly,come,could,d,did,differ,different,differently,do,does,done,down,down,downed,downing,downs,during,e,each,early,either,end,ended,ending,ends,enough,even,evenly,ever,every,everybody,everyone,everything,everywhere,f,face,faces,fact,facts,far,felt,few,find,finds,first,for,four,from,full,fully,further,furthered,furthering,furthers,g,gave,general,generally,get,gets,give,given,gives,go,going,good,goods,got,great,greater,greatest,group,grouped,grouping,groups,h,had,has,have,having,he,her,here,herself,high,high,high,higher,highest,him,himself,his,how,however,i,if,important,in,interest,interested,interesting,interests,into,is,it,its,itself,j,just,k,keep,keeps,kind,knew,know,known,knows,l,large,largely,last,later,latest,least,less,let,lets,like,likely,long,longer,longest,m,made,make,making,man,many,may,me,member,members,men,might,more,most,mostly,mr,mrs,much,must,my,myself,n,necessary,need,needed,needing,needs,never,new,new,newer,newest,next,no,nobody,non,noone,not,nothing,now,nowhere,number,numbers,o,of,off,often,old,older,oldest,on,once,one,only,open,opened,opening,opens,or,order,ordered,ordering,orders,other,others,our,out,over,p,part,parted,parting,parts,per,perhaps,place,places,point,pointed,pointing,points,possible,present,presented,presenting,presents,problem,problems,put,puts,q,quite,r,rather,really,right,right,room,rooms,s,said,same,saw,say,says,second,seconds,see,seem,seemed,seeming,seems,sees,several,shall,she,should,show,showed,showing,shows,side,sides,since,small,smaller,smallest,so,some,somebody,someone,something,somewhere,state,states,still,still,such,sure,t,take,taken,than,that,the,their,them,then,there,therefore,these,they,thing,things,think,thinks,this,those,though,thought,thoughts,three,through,thus,to,today,together,too,took,toward,turn,turned,turning,turns,two,u,under,until,up,upon,us,use,used,uses,v,very,w,want,wanted,wanting,wants,was,way,ways,we,well,wells,went,were,what,when,where,whether,which,while,who,whole,whose,why,will,with,within,without,work,worked,working,works,would,x,y,year,years,yet,you,young,younger,youngest,your,yours,z,et,al,stop\",\"expand_ontologies\":\"1150\",\"command\":\"job\"}").and_return(true)
        @s.send_job("1234", job, dataset).should be_true
      end
    end

    describe "without worker" do
      it "should fail to send a job if no wo" do
        dataset = Dataset.new
        job = Job.new
        @s.should_receive(:get_worker).with("1234").and_return(nil)
        job.should_receive(:failed).and_return(true)
        @s.should_receive(:publish).with("1234", "{\"command\":\"shutdown\"}").and_return(true)
        @s.send_job("1234", job, dataset).should be_true
      end
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
      Job.should_receive(:first).with(:conditions=>{:id=>"12"}).and_return(job)
      job.should_receive(:working).and_return(true)
      @s.working_job("1234","12")
    end
  end

  describe "finished_job" do
    it "should update the job attributes" do
      job = Job.new
      Job.should_receive(:first).with(:conditions=>{:id=>"12"}).and_return(job)
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
      Job.should_receive(:first).with(:conditions=>{:id=>"12"}).and_return(job)
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
      EM.should_receive(:add_periodic_timer).with(5).and_yield#("timer")
    end

    describe "if needed" do
      it "should launch more" do
        Worker.should_receive(:available).with(:count => true).and_return(0)
        Worker.should_receive(:count).and_return(5)
        Job.should_receive(:available).with(:count => true).and_return(5)
        @s.should_receive(:worker_max).and_return(10)
        @s.node_queue.should_receive(:publish).with({'command' => 'launch'}.to_json, :persistent => true).and_return(true)
        @s.launch_timer
      end

      it "should launch more for 1 job" do
        Worker.should_receive(:available).with(:count => true).and_return(0)
        Worker.should_receive(:count).and_return(5)
        Job.should_receive(:available).with(:count => true).and_return(1)
        @s.should_receive(:worker_max).and_return(10)
        @s.node_queue.should_receive(:publish).with({'command' => 'launch'}.to_json, :persistent => true).and_return(true)
        @s.launch_timer
      end
    end

    describe "not needed" do
      it "should not launch more if workers available" do
        Worker.should_receive(:available).with(:count => true).and_return(1)
        Worker.should_receive(:count).and_return(4)
        Job.should_receive(:available).with(:count => true).and_return(2)
        @s.should_receive(:worker_max).and_return(5)
        @s.should_not_receive(:publish)
        @s.launch_timer
      end

      it "should not launch more if at worker max" do
        Worker.should_receive(:available).with(:count => true).and_return(0)
        Worker.should_receive(:count).and_return(5)
        Job.should_receive(:available).with(:count => true).and_return(2)
        @s.should_receive(:worker_max).and_return(5)
        @s.should_not_receive(:publish)
        @s.launch_timer
      end

      it "should not launch more if no more jobs" do
        Worker.should_receive(:available).with(:count => true).and_return(0)
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

            @platform = Gminer::Platform.new
            @platform.should_receive(:description).and_return("")

            @job.should_receive(:field_name).and_return("description")
            @job.should_receive(:geo_accession).and_return("GPL1355")
            Job.should_receive(:load_item).with("GPL1355").and_return(@platform)
            @job.should_receive(:has_blank_field).and_return(true)
            @s.should_receive(:free_or_shutdown).with("1234").and_return(true)

            @s.start_job("1234")
          end
        end

        describe "without blank field value" do
          before(:each) do
            worker = Worker.new
            @s.should_receive(:get_worker).with("1234").and_return(worker)
            @job.stub!(:id).and_return(12)
            @job.stub!(:field_name).and_return("description")
            @job.stub!(:geo_accession).and_return("GPL1355")
            @job.should_receive(:started).with("1234").and_return(true)
            @platform = Gminer::Platform.new
            @platform.should_receive(:description).and_return("platform")
            Job.should_receive(:load_item).with("GPL1355").and_return(@platform)
            @ontology = Ontology.new
            @job.stub!(:ontology).and_return(@ontology)
          end

          it "should process the job" do
            @s.should_receive(:send_job).with("1234", @job, @platform).and_return(true)
            @s.start_job("1234")
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
      @s.should_receive(:publish).with("1234", "{\"command\":\"shutdown\"}").and_return(true)
      worker = Worker.new
      Worker.should_receive(:first).with(:conditions => {:worker_key=> "1234"}).and_return(worker)
      worker.should_receive(:destroy).and_return(true)
      @s.no_jobs("1234")
    end
  end

end
