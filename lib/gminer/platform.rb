module Gminer
  class Platform < ActiveRecord::Base
  include Utilities

  has_many :series_items
  has_many :samples, :through => :series_items

  end
end
