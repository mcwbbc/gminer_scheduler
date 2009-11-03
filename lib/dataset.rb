class Dataset < ActiveRecord::Base
  include Utilities

  belongs_to :platform, :foreign_key => :platform_geo_accession
  belongs_to :series_item, :foreign_key => :reference_series, :primary_key => :geo_accession

end
