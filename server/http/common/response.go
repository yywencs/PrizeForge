package common

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Response struct {
	Code int         `json:"code"`
	Info string      `json:"info"`
	Data interface{} `json:"data"`
}

func Success(ctx *gin.Context, data interface{}) {
	ctx.JSON(http.StatusOK, Response{
		Code: 0,
		Info: "success",
		Data: data,
	})
}

func Error(ctx *gin.Context, code int, info string) {
	ctx.JSON(http.StatusOK, Response{
		Code: code,
		Info: info,
		Data: nil,
	})
}
