require File.dirname(__FILE__) + '/../spec_helper'

describe Worker do
  describe "available" do
    it "should count the available workers" do
      Worker.should_receive(:count).with(:conditions => {:working => false, :ready => true}).and_return(1)
      Worker.available(:count => true).should == 1
    end

    it "should find the first available worker" do
      worker = Worker.new(:worker_key => 'worker-1234')
      Worker.should_receive(:first).with(:conditions => {:working => false, :ready => true}).and_return(worker)
      Worker.available.should == worker
    end
  end

  describe "free" do
    it "should set the worker to working false and send a prepare message" do
      worker = Worker.new
      worker.should_receive(:save!).and_return(true)
      worker.free.should be_true
    end
  end

  describe "working" do
    it "should set the worker to working true" do
      worker = Worker.new
      worker.should_receive(:save!).and_return(true)
      worker.working.should be_true
    end
  end

  describe "crashed?" do
    it "should be true if more than 2 minutes ago" do
      w = Worker.new(:updated_at => 3.minutes.ago)
      w.crashed?.should == true
    end

    it "should be false if less than 2 minutes ago" do
      w = Worker.new(:updated_at => 1.minutes.ago)
      w.crashed?.should == false
    end
  end


end

