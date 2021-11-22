package api

import (
	"github.com/labstack/echo/v4"
	"net/http"
	"os/exec"
)

const collectInfoPath = "/collect-information.sh"

func (s *Server) PostCollectInformation(ctx echo.Context) error {
	cmd := exec.Command(collectInfoPath)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	err = cmd.Start()
	if err != nil {
		return err
	}
	return ctx.Stream(http.StatusOK, "text/plain", stdout)
}
