package frontend

import (
	"log"
	"net/http"
	"os"
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
	templatePath, _ := filepath.Abs("frontend/layout.html")
	log.Println(templatePath)

	ex, err := os.Executable()
	if err != nil {
		log.Println(err)
	}
	exPath := filepath.Dir(ex)
	log.Println(exPath)

	tmpl := template.Must(template.ParseFiles(templatePath))
	tmpl.Execute(w, dataResp)
}
