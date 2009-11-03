require File.join( File.dirname(__FILE__), '..', "spec_helper" )

describe Utilities do

  class FakeClass
    include Utilities
  end

  describe "descriptive_text" do
    before(:each) do
      @fake = FakeClass.new
      @fake.should_receive(:title).and_return("title")
    end

    it "should return a sample" do
      series = mock(SeriesItem, :title => "seriestitle")
      @fake.should_receive(:series_item).and_return(series)
      @fake.should_receive(:geo_accession).and_return("GSM1234")
      @fake.descriptive_text.should == "seriestitle - title"
    end

    it "should return a series" do
      @fake.should_receive(:geo_accession).and_return("GSE1234")
      @fake.descriptive_text.should == "title"
    end

    it "should return a platform" do
      @fake.should_receive(:geo_accession).and_return("GPL1234")
      @fake.descriptive_text.should == "title"
    end

    it "should return a dataset" do
      @fake.should_receive(:geo_accession).and_return("GDS1234")
      @fake.descriptive_text.should == "title"
    end
  end

end
