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

	"vitess.io/vitess/go/vt/log"
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

func (postponedFuncsContainer *PostponedFunctionsContainer) AddPostponedFunction(postponedFunction func() error, description string) {
	postponedFuncsContainer.mutex.Lock()
	defer postponedFuncsContainer.mutex.Unlock()

	postponedFuncsContainer.descriptions = append(postponedFuncsContainer.descriptions, description)

	postponedFuncsContainer.waitGroup.Add(1)
	go func() {
		defer postponedFuncsContainer.waitGroup.Done()
		_ = postponedFunction()
	}()
}

func (postponedFuncsContainer *PostponedFunctionsContainer) Wait() {
	log.Infof("PostponedFunctionsContainer: waiting on %+v postponed functions", postponedFuncsContainer.Len())
	postponedFuncsContainer.waitGroup.Wait()
	log.Infof("PostponedFunctionsContainer: done waiting")
}

func (postponedFuncsContainer *PostponedFunctionsContainer) Len() int {
	postponedFuncsContainer.mutex.Lock()
	defer postponedFuncsContainer.mutex.Unlock()

	return len(postponedFuncsContainer.descriptions)
}

func (postponedFuncsContainer *PostponedFunctionsContainer) Descriptions() []string {
	postponedFuncsContainer.mutex.Lock()
	defer postponedFuncsContainer.mutex.Unlock()

	return postponedFuncsContainer.descriptions
}
