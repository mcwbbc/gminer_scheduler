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
      Worker.should_receive(:update_all).with({:working => false}, :worker_key => "1234").and_return(true)
      Worker.free("1234").should be_true
    end
  end

  describe "working" do
    it "should set the worker to working true" do
      Worker.should_receive(:update_all).with({:working => true}, :worker_key => "1234").and_return(true)
      Worker.working("1234").should be_true
    end
  end
end
