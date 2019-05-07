#!/usr/bin/ruby
# Encoding: utf-8
#
# Copyright:: Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This code example illustrates how to get metrics about a campaign and 
# serialize the result as a CSV file.
require 'rubygems'
require 'optparse'
require 'active_support/all'
require 'curl'
require 'net/http'
require 'uri'
require 'json'
require 'time'
require 'csv'
require 'google/ads/google_ads'
require 'dotenv'

Dotenv.load

PAGE_SIZE = 1000


ACCOUNT_ID = ENV['ACCOUNT_ID']
AIRLINE_CODE = ENV['AIRLINE_CODE']
LOOK_AHEAD_WINDOW = ENV['LOOK_AHEAD_WINDOW']
JOURNEY_TYPE = ENV['JOURNEY_TYPE']
CURRENCY = ENV['CURRENCY']
def query_ad_group_ads_with_clicks(customer_id)
  client = Google::Ads::GoogleAds::GoogleAdsClient.new
  
  ga_service = client.service(:GoogleAds)

  query = <<~QUERY
  SELECT
    ad_group.id,
    ad_group.name,
    metrics.clicks
  FROM
    ad_group_ad
  WHERE
    metrics.clicks = 0 AND ad_group_ad.status = 'ENABLED'
  LIMIT
    100
  QUERY

  response = ga_service.search(customer_id, query, page_size: PAGE_SIZE)
  adgroups = response.map { |row| row['ad_group'].id.value }.uniq!

  adgroups

end

def write_business_feed_report(customer_id, target_filepath)
  sanitized_customer_id = customer_id.tr("-", "")
  client = Google::Ads::GoogleAds::GoogleAdsClient.new
  
  ga_service = client.service(:GoogleAds)
  
  query = <<~QUERY
  SELECT
    feed.id,
    feed.name,
    feed_item.attribute_values,
    metrics.clicks
  FROM
    feed_item
  WHERE
    feed.name = 'DPI_#{customer_id}' AND metrics.clicks > 0
  ORDER BY
    metrics.clicks DESC
  LIMIT
    1000
  QUERY

  response = ga_service.search(sanitized_customer_id, query, page_size: PAGE_SIZE)

  # convert the Google Ads response rows in to CSV ready hash objects
  csv_rows = response.map { |row| result_row_as_hash(row) }.compact
  discrepancies = csv_rows.select { |row| row[:diff] > 5}

  rate = (discrepancies.size.to_f / csv_rows.size.to_f ) * 100
  csv_rows.push({:origin => "* Total #{csv_rows.size}"})
  csv_rows.push({:origin => "* Total Discrepancies #{discrepancies.size}"})
  csv_rows.push({:origin => "* Rate: #{rate}"})
  CSV.open(target_filepath, "wb") do |csv|
    # use the keys of the first csv_row as a header row
    csv << csv_rows.first.keys
    
    # write all the values as rows of the CSV
    csv_rows.each do |row|
      csv << row.values
    end
  end unless csv_rows.size == 0
end

def result_row_as_hash(row)
  origin = select_column_values(row, 2).string_value.value
  destination = select_column_values(row, 3).string_value.value
  price = select_column_values(row, 4).string_value.value
  # puts price
  # clicks = row.metrics.clicks
  body = define_sputnik_body(origin, destination, LOOK_AHEAD_WINDOW, JOURNEY_TYPE, CURRENCY);
  trfx_result = microservice_airfare_call(AIRLINE_CODE, body)
  thousand_separator = "."
  price_to_i = price.split(thousand_separator)[0].gsub(/\D/,"").to_i
  # puts trfx_result.inspect
  trfx_price = trfx_result.size > 0 ? trfx_result[0]["totalPrice"] : 0
  diff_price = (price_to_i - trfx_price).abs
    {
      :origin => origin,
      :destination => destination,
      :feedPrice => price_to_i,
      :trfx =>  trfx_price,
      :diff => diff_price
    }
end

def select_column_values row, attribute_id_value
  row.feed_item.attribute_values.find{|attribute| attribute.feed_attribute_id.value == attribute_id_value}
end

def define_sputnik_body(origin, destination, plus_days, journey_type, currency)
  
  now = Date.today
  plus_n_days = (now + plus_days.to_i)
  body  = {
        "flightType" => journey_type,
        "priceFormat" => {"decimalSeparator" => ".", "thousandSeparator" => "," ,"decimalPlaces" => 0},
        "datePattern" =>"MM/dd/yyyy",
        "languageCode" => "en",
        "outputCurrencies"=> [currency],
        "faresPerRoute"=> 30,
        "routesLimit" => 10,
        "dataExpirationWindow" => ENV['DATA_EXPIRATION_WINDOW'], #custom by airline
        "outputFields" => ["returnDate","usdTotalPrice","popularity","originCity","destinationCity","destinationAirportImage","destinationCityImage","destinationStateImage","destinationCountryImage","destinationRegionImage","farenetTravelClass","travelClass","flightDeltaDays","siteEdition"],
        "sorting" => [],
        "departure" => {
          "start" => now.to_s,
          "end" => plus_n_days.to_s
        },
        "page" => 1,
        "origins" => [origin],
        "destinations" => [destination],
        "faresLimit" => 7
  }
  body
end

def microservice_airfare_call airline, body
  url = "#{ENV['MICROSERVICES_URL']}/airfare-sputnik-service/#{ENV['MS_VERSION']}/#{AIRLINE_CODE}/fares/aggregation"
  header = {'Content-Type': 'text/json'}
  begin
    uri = URI(url)
    http = Net::HTTP.new(uri.host, uri.port)
    http.use_ssl = true
    credentials = ENV['MS_TOKEN']
    req = Net::HTTP::Post.new(uri.path, {'Content-Type' =>'application/json',  
            'Authorization' => "Bearer #{credentials}"})
    req.body = body.to_json
    res = http.request(req)
  rescue => e
    puts "failed #{e}"
  end
  JSON.parse(res.body)
end

if __FILE__ == $PROGRAM_NAME
  options = {}
  puts "Start QA for #{AIRLINE_CODE.upcase} in account #{ACCOUNT_ID}"
  Dir.mkdir 'REPORTS' unless File.exists?('REPORTS')
  file_name = "REPORTS/#{File.basename(__FILE__)}"
  # The following parameter(s) should be provided to run the example. Please
  # create .env file with the missing values
  # the command line.
  #
  # Parameters passed on the command line will override any parameters set in
  # code.
  #
  # Running the example with -h will print the command line usage.
  options[:customer_id] = ACCOUNT_ID

  options[:output_file_path] = file_name.gsub(".rb", "_#{AIRLINE_CODE.upcase}.csv")
  # OptionParser.new do |opts|
  #   opts.banner = sprintf('Usage: ruby %s [options]', File.basename(__FILE__))

  #   opts.separator ''
  #   opts.separator 'Options:'

  #   opts.on('-C', '--customer-id CUSTOMER-ID', String, 'Customer ID') do |v|
  #     options[:customer_id] = v
  #   end

  #   opts.on('-O', '--output-file-path OUTPUT-FILE-PATH', String, 'Output File Path') do |v|
  #     options[:output_file_path] = v
  #   end

  #   opts.separator ''
  #   opts.separator 'Help:'

  #   opts.on_tail('-h', '--help', 'Show this message') do
  #     puts opts
  #     exit
  #   end
  # end.parse!

  begin
    # the GoogleAdsClient only accepts customer IDs without `-` characters,
    # so this removes them if the caller of this script copy pasted a customer
    # id directly from the user interface
    
    # http = Curl.post("http://www.google.com", {}) do |http|
    #   puts "http #{http}"

    # end
    write_business_feed_report(options.fetch(:customer_id), options.fetch(:output_file_path))
  rescue Google::Ads::GoogleAds::Errors::GoogleAdsError => e
    e.failure.errors.each do |error|
      STDERR.printf("Error with message: %s\n", error.message)
      if error.location
        error.location.field_path_elements.each do |field_path_element|
          STDERR.printf("\tOn field: %s\n", field_path_element.field_name)
        end
      end
      error.error_code.to_h.each do |k, v|
        next if v == :UNSPECIFIED
        STDERR.printf("\tType: %s\n\tCode: %s\n", k, v)
      end
    end
  rescue Google::Gax::RetryError => e
    STDERR.printf("Error: '%s'\n\tCause: '%s'\n\tCode: %d\n\tDetails: '%s'\n" \
        "\tRequest-Id: '%s'\n", e.message, e.cause.message, e.cause.code,
        e.cause.details, e.cause.metadata['request-id'])
  end
end