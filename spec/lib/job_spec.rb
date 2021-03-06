require 'spec_helper'

describe Job do

  describe "started" do
    it "should update the job status to started" do
      job = Job.new
      Time.stub!(:now).and_return(123.123)
      job.should_receive(:update_attributes).with(:worker_key => "key", :started_at => 123.123).and_return(true)
      job.started("key").should be_true
    end
  end

  describe "working" do
    it "should update the job status to working" do
      job = Job.new
      Time.stub!(:now).and_return(123.123)
      job.should_receive(:update_attributes).with(:working_at => 123.123).and_return(true)
      job.working.should be_true
    end
  end

  describe "finished" do
    it "should update the job status to finished" do
      job = Job.new
      Time.stub!(:now).and_return(123.123)
      job.should_receive(:update_attributes).with(:worker_key => nil, :finished_at => 123.123).and_return(true)
      job.finished.should be_true
    end
  end

  describe "failed" do
    it "should update the job status to failed" do
      job = Job.new
      job.should_receive(:update_attributes).with(:started_at => nil, :finished_at => nil, :working_at => nil, :worker_key => nil).and_return(true)
      job.failed.should be_true
    end
  end

  describe "available" do
    before(:each) do
      @one = Job.new
    end

    it "should return the first available job" do
      # (worker_key IS NULL AND (finished_at IS NULL OR finished_at < 2.weeks.ago)) OR (worker_key IS NOT NULL AND started_at < 5.minutes.ago)
      right_now = Time.now.to_f
      Job.should_receive(:first).with(:conditions => ["(worker_key IS NULL AND finished_at IS NULL) OR (worker_key IS NOT NULL AND ((started_at IS NULL) OR (started_at < ?))) OR (worker_key IS NULL AND finished_at IS NOT NULL AND ((working_at IS NULL) OR (started_at IS NULL)))", right_now]).and_return(@one)
      Job.available(:expired_at => right_now, :crashed_at => right_now).should == @one
    end

    it "should return the count of available jobs" do
      right_now = Time.now.to_f
      Job.should_receive(:count).with(:conditions => ["(worker_key IS NULL AND finished_at IS NULL) OR (worker_key IS NOT NULL AND ((started_at IS NULL) OR (started_at < ?))) OR (worker_key IS NULL AND finished_at IS NOT NULL AND ((working_at IS NULL) OR (started_at IS NULL)))", right_now]).and_return(10)
      Job.available(:expired_at => right_now, :crashed_at => right_now, :count => true).should == 10
    end
  end

end
