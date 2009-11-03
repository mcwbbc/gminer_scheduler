class Sample < ActiveRecord::Base
  include Utilities

  belongs_to :series_item
  belongs_to :platform

end
