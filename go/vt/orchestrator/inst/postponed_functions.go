/*
   Copyright 2015 Shlomi Noach, courtesy Booking.com

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

package inst

import (
	"sync"

	"vitess.io/vitess/go/vt/orchestrator/external/golib/log"
)

type PostponedFunctionsContainer struct {
	waitGroup    sync.WaitGroup
	mutex        sync.Mutex
	descriptions []string
}

func NewPostponedFunctionsContainer() *PostponedFunctionsContainer {
	postponedFunctionsContainer := &PostponedFunctionsContainer{
		descriptions: []string{},
	}
	return postponedFunctionsContainer
}

func (this *PostponedFunctionsContainer) AddPostponedFunction(postponedFunction func() error, description string) {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	this.descriptions = append(this.descriptions, description)

	this.waitGroup.Add(1)
	go func() {
		defer this.waitGroup.Done()
		postponedFunction()
	}()
}

func (this *PostponedFunctionsContainer) Wait() {
	log.Debugf("PostponedFunctionsContainer: waiting on %+v postponed functions", this.Len())
	this.waitGroup.Wait()
	log.Debugf("PostponedFunctionsContainer: done waiting")
}

func (this *PostponedFunctionsContainer) Len() int {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	return len(this.descriptions)
}

func (this *PostponedFunctionsContainer) Descriptions() []string {
	this.mutex.Lock()
	defer this.mutex.Unlock()

	return this.descriptions
}
