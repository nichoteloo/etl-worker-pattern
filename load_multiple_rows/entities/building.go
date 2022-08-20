package entities

import (
	"time"
)

// Structs
type Building struct {
	BASE_BBL    string    
	MPLUTO_BBL  string    
	BIN         float64   
	NAME        string    
	LSTMODDATE  time.Time 
	LSTSTATTYPE string    
	CNSTRCT_YR  float64   
	DOITT_ID    float64   
	HEIGHTROOF  float64   
	FEAT_CODE   float64  
	GROUNDELEV  float64   
	GEOM_SOURCE string    
}