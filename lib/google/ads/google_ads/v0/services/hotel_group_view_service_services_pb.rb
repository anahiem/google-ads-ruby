# Generated by the protocol buffer compiler.  DO NOT EDIT!
# Source: google/ads/google_ads/v0/services/hotel_group_view_service.proto for package 'google.ads.googleads.v0.services'
# Original file comments:
# Copyright 2018 Google LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#

require 'grpc'
require 'google/ads/google_ads/v0/services/hotel_group_view_service_pb'

module Google
  module Ads
    module GoogleAds
      module V0
        module Services
          module HotelGroupViewService
            # Service to manage Hotel Group Views.
            class Service

              include GRPC::GenericService

              self.marshal_class_method = :encode
              self.unmarshal_class_method = :decode
              self.service_name = 'google.ads.googleads.v0.services.HotelGroupViewService'

              # Returns the requested Hotel Group View in full detail.
              rpc :GetHotelGroupView, GetHotelGroupViewRequest, Google::Ads::GoogleAds::V0::Resources::HotelGroupView
            end

            Stub = Service.rpc_stub_class
          end
        end
      end
    end
  end
end
