package boost

import (
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

func Load(configPath *string) (*QueryFilterConfigs, error) {
	data, err := ioutil.ReadFile(*configPath)
	fmt.Printf("Reading Boost Configs from: %s\n", *configPath)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		return nil, err
	}

	var configs QueryFilterConfigs
	err = yaml.Unmarshal(data, &configs)
	if err != nil {
		return nil, err
	}

	fmt.Printf("Parsed Configs: %+v\n", configs)

	rocket := `
       |
      / \
     / _ \
    |.'''.|
    |'._.'|
    |BOOST|
   ,'|  | |\.
  /  |  | |  \
  |,-'--|--'-.|
	|| || ||	
`

	fmt.Println(rocket)

	fmt.Println("Boost enabled.")

	return &configs, nil
}
