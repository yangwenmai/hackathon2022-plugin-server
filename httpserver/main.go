package main

import (
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/pingcap/tiflow/cdc/model"
	"log"
	"net/http"
)

// AddTableReq add table request
type AddTableReq struct {
	TableID int64 `json:"table_id"`
}

// RemoveTableReq remove table request
type RemoveTableReq struct {
	TableID int64 `json:"table_id"`
}

type PluginRequest struct {
	Operation string      `json:"operation"`
	Data      interface{} `json:"data"`
}

var (
	sinkAddTable             = "sink_add_table"
	sinkRemoveTable          = "sink_remove_table"
	sinkEmitRowChangedEvents = "sink_emit_row_changed_events"
	sinkEmitDDLEvent         = "sink_emit_ddl_event"
)

func main() {
	r := gin.Default()
	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"code":    200,
			"message": "pong",
		})
	})
	r.POST("/sink_sync", func(c *gin.Context) {
		pr := &PluginRequest{}
		if err := c.ShouldBindJSON(pr); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"code":    400,
				"message": err.Error(),
			})
			return
		}
		rd, err := json.Marshal(pr.Data)
		if err != nil {
			log.Printf("marshal failed, error: %v", err)
			return
		}
		switch pr.Operation {
		case sinkAddTable:
			AddTable(rd)
		case sinkRemoveTable:
			RemoveTable(rd)
		case sinkEmitRowChangedEvents:
			EmitRowChangedEvents(rd)
		case sinkEmitDDLEvent:
			EmitDDLEvent(rd)
		default:
			log.Printf("unsupport operation")
			c.JSON(http.StatusBadRequest, gin.H{
				"code":    400,
				"message": "unsupport operation",
				"cause":   fmt.Sprintf("We havn't support operation %v", pr.Operation),
			})
			return
		}
		c.JSON(http.StatusOK, gin.H{
			"code":    200,
			"message": fmt.Sprintf("%v ok", pr.Operation),
		})
	})
	r.Run(":5005")
}

func AddTable(data []byte) {
	log.Printf("start execute add table")
	atr := &AddTableReq{}
	err := json.Unmarshal(data, atr)
	if err != nil {
		log.Printf("addTable unmarshal failed, error: %v", err)
		return
	}
	log.Printf("req data: %v", atr)
	log.Printf("execute add table end")
}

func RemoveTable(data []byte) {
	log.Printf("start execute remove table")
	rtr := &RemoveTableReq{}
	err := json.Unmarshal(data, rtr)
	if err != nil {
		log.Printf("RemoveTable unmarshal failed, error: %v", err)
		return
	}
	log.Printf("req data: %v", rtr)
	log.Printf("execute remove table end")
}

func EmitRowChangedEvents(data []byte) {
	log.Printf("start execute row changed events")
	rces := []*model.RowChangedEvent{}
	err := json.Unmarshal(data, &rces)
	if err != nil {
		log.Printf("EmitRowChangedEvents unmarshal failed, error: %v", err)
		return
	}
	for i, rce := range rces {
		log.Printf("index: %v, table: %v", i, rce.Table)
		for j, column := range rce.Columns {
			log.Printf("column index: %v, column value: %v", j, column.Value)
		}
	}
	log.Printf("execute row changed events end")
}

func EmitDDLEvent(data []byte) {
	log.Printf("start execute ddl event")
	ddlEvent := &model.DDLEvent{}
	err := json.Unmarshal(data, ddlEvent)
	if err != nil {
		log.Printf("RemoveTable unmarshal failed, error: %v", err)
		return
	}
	log.Printf("req data: %v", ddlEvent)
	log.Printf("execute ddl event end")
}
