package frontend

import (
	"log"
	"net/http"
	"path/filepath"
	"text/template"

	"github.com/JimbiniBambini/exchanges_data_polling/managers/api_manager"
)

type Todo struct {
	Title string
	Done  bool
}

type TodoPageData struct {
	PageTitle string
	Todos     []Todo
}

func Display(w http.ResponseWriter, r *http.Request, dataResp api_manager.Responder) {
	templatePath, _ := filepath.Abs("../frontend/layout.html")
	log.Println(templatePath)
	tmpl := template.Must(template.ParseFiles(templatePath))
	tmpl.Execute(w, dataResp)
}
