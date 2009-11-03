class SeriesItem < ActiveRecord::Base
  include Utilities

  belongs_to :platform
  has_many :samples

end
