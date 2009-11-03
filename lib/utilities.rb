module Utilities

  def descriptive_text
    case self.geo_accession
      when /^GSM/
        "#{self.series_item.title} - #{self.title}"
      when /^GSE/
        self.title
      when /^GPL/
        self.title
      when /^GDS/
        self.title
    end
  end

end