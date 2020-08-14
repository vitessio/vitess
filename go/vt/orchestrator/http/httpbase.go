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

package http

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/martini-contrib/auth"

	"vitess.io/vitess/go/vt/orchestrator/config"
	"vitess.io/vitess/go/vt/orchestrator/inst"
	"vitess.io/vitess/go/vt/orchestrator/os"
	"vitess.io/vitess/go/vt/orchestrator/process"
	orcraft "vitess.io/vitess/go/vt/orchestrator/raft"
)

func getProxyAuthUser(req *http.Request) string {
	for _, user := range req.Header[config.Config.AuthUserHeader] {
		return user
	}
	return ""
}

// isAuthorizedForAction checks req to see whether authenticated user has write-privileges.
// This depends on configured authentication method.
func isAuthorizedForAction(req *http.Request, user auth.User) bool {
	if config.Config.ReadOnly {
		return false
	}

	if orcraft.IsRaftEnabled() && !orcraft.IsLeader() {
		// A raft member that is not a leader is unauthorized.
		return false
	}

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			// The mere fact we're here means the user has passed authentication
			return true
		}
	case "multi":
		return string(user) != "readonly"
	case "proxy":
		{
			authUser := getProxyAuthUser(req)
			for _, configPowerAuthUser := range config.Config.PowerAuthUsers {
				if configPowerAuthUser == "*" || configPowerAuthUser == authUser {
					return true
				}
			}
			// check the user's group is one of those listed here
			if len(config.Config.PowerAuthGroups) > 0 && os.UserInGroups(authUser, config.Config.PowerAuthGroups) {
				return true
			}
			return false
		}
	case "token":
		{
			cookie, err := req.Cookie("access-token")
			if err != nil {
				return false
			}

			publicToken := strings.Split(cookie.Value, ":")[0]
			secretToken := strings.Split(cookie.Value, ":")[1]
			result, _ := process.TokenIsValid(publicToken, secretToken)
			return result
		}
	case "oauth":
		{
			return false
		}
	default:
		{
			// Default: no authentication method
			return true
		}
	}
}

func authenticateToken(publicToken string, resp http.ResponseWriter) error {
	secretToken, err := process.AcquireAccessToken(publicToken)
	if err != nil {
		return err
	}
	cookieValue := fmt.Sprintf("%s:%s", publicToken, secretToken)
	cookie := &http.Cookie{Name: "access-token", Value: cookieValue, Path: "/"}
	http.SetCookie(resp, cookie)
	return nil
}

// getUserId returns the authenticated user id, if available, depending on authertication method.
func getUserId(req *http.Request, user auth.User) string {
	if config.Config.ReadOnly {
		return ""
	}

	switch strings.ToLower(config.Config.AuthenticationMethod) {
	case "basic":
		{
			return string(user)
		}
	case "multi":
		{
			return string(user)
		}
	case "proxy":
		{
			return getProxyAuthUser(req)
		}
	case "token":
		{
			return ""
		}
	default:
		{
			return ""
		}
	}
}

func getClusterHint(params map[string]string) string {
	if params["clusterHint"] != "" {
		return params["clusterHint"]
	}
	if params["clusterName"] != "" {
		return params["clusterName"]
	}
	if params["host"] != "" && params["port"] != "" {
		return fmt.Sprintf("%s:%s", params["host"], params["port"])
	}
	return ""
}

// figureClusterName is a convenience function to get a cluster name from hints
func figureClusterName(hint string) (clusterName string, err error) {
	if hint == "" {
		return "", fmt.Errorf("Unable to determine cluster name by empty hint")
	}
	instanceKey, _ := inst.ParseRawInstanceKey(hint)
	return inst.FigureClusterName(hint, instanceKey, nil)
}

// getClusterNameIfExists returns a cluster name by params hint, or an empty cluster name
// if no hint is given
func getClusterNameIfExists(params map[string]string) (clusterName string, err error) {
	if clusterHint := getClusterHint(params); clusterHint == "" {
		return "", nil
	} else {
		return figureClusterName(clusterHint)
	}
}
