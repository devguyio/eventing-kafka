/*
Copyright 2018 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package eventing

import "k8s.io/apimachinery/pkg/runtime/schema"

const (
	GroupName = "eventing.knative.dev"

	// BrokerLabelKey is the label key on Triggers and Subscriptions
	// to indicate to which Broker they belong to.
	BrokerLabelKey = GroupName + "/broker"

	// BrokerClassKey is the annotation key on Brokers to indicate
	// which Controller is responsible for them.
	BrokerClassKey = GroupName + "/broker.class"
)

var (
	// TriggersResource respresents a Knative Trigger
	TriggersResource = schema.GroupResource{
		Group:    GroupName,
		Resource: "triggers",
	}
	// BrokersResource respresents a Knative Broker
	BrokersResource = schema.GroupResource{
		Group:    GroupName,
		Resource: "brokers",
	}
)
