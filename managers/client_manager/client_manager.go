package client_manager

import (
	"context"
	"encoding/json"

	"github.com/JimbiniBambini/exchanges_data_polling/clients/storj_client"
	"github.com/JimbiniBambini/exchanges_data_polling/common"
	"github.com/JimbiniBambini/exchanges_data_polling/workers"
)

/* ****************************************** CLIENT IMPLEMENTATION ****************************************** */
type Client struct {
	ID          string
	StorjClient storj_client.StorjClient
	Workers     map[string]map[string]*workers.AssetWorker //scheme: bucket --> worker_id
}

type ClientManager struct {
	Clients map[string]*Client
}

func NewClientManager() ClientManager {
	var clientManager ClientManager
	clientManager.Clients = make(map[string]*Client, 0)
	return clientManager
}

func (clientManager *ClientManager) LoginHandler(loginCredentials map[string]string) string {
	success := "error"
	mapBytes, _ := json.Marshal(loginCredentials)
	newID := common.CalcRequestBodyCheckSum(mapBytes)
	alreadyAvailable := false
	for _, cli := range clientManager.Clients {
		if cli.ID == newID {
			alreadyAvailable = true
			break
		}
	}
	if !alreadyAvailable {
		ctx := context.Background()

		newClient, cliAvailable := storj_client.NewStorjClient(ctx, loginCredentials)
		if cliAvailable {
			clientManager.Clients[newID] = &Client{
				ID:          newID,
				StorjClient: newClient,
				Workers:     make(map[string]map[string]*workers.AssetWorker, 0),
			}
			success = newID
		}
	}

	return success
}
