* Esta es la version re modificada
/*-------------------------------------------------------*/
/*		 [>  Cleaning Survey CTO and reports - dataset  <]

Author: @samuelarispe
Survey form version: MC_2018_verificacion_encuestadores_newversion_v1.dta
*/
/*----------------------------------------------------*/


* Setting

dis "`c(hostname)'"

local machine "`c(hostname)'" // anyone who is working could add his/her
	// hostname 

* In one of the next rows you can add your directory

	// note tha here every space is a ";" I dont like very big
	// sentence in a row. It looks terrible :)

* Note in who you need to put your initials (e.g: Paulo Matos - PM)


#d ; 

	if "`machine'" == "Paulos-MacBook-Pro.local" {;
		local db "/Users/paulomatos/Dropbox";
		local who "PM";
	};

	if "`machine'" == "Medina-PC" {;
		local db "C:/Users/Medina/Dropbox";
		local who "DM";
	};

	if "`machine'" == "LP100852" { ;
		local db "/Users/p.villaparo/Dropbox";
		local who "PV";
	};

	if "`machine'" == "Samuel-PC" { ;
		local db "C:/Users/Samuel/Dropbox";
		local who "SA";
	};

	if "`machine'" == "PC0661ZE" { ;
		local db "C:/Users/sarispe/Dropbox";
		local who "SA";
	};


#d cr

* Working directory

set more off

local mc "Proyecto Matching Contribution 2017-2018"
local bl "`db'/`mc'/07_Questionnaires&Data/Baseline_Quant"
local monitor "interim/outputs/enumerators"


cd "`bl'" // Set working directory the BaseLine


* Other datasets

local randomization "`db'/`mc'/04_ResearchDesign/03 Randomization"
local randomization "`randomization'/PROYECTO/data"
*local randomization ///
*	"`randomization'/matching_randomized_11196_1208_recoding.dta"
local randomization ///
	"`randomization'/matching_randomized_12588_13893_11196_1208.dta"
local rand_geo "`db'/`mc'/04_ResearchDesign/03 Randomization"
local rand_geo "`rand_geo'/PROYECTO/data/Samuel"

local pilot_data "archive/baseline_matching.dta"
local project_data "interim/matching_contributions_v1_clean.dta"
 
	//data help of Samuel Arispe 
local enumerator "raw/enumerators/Flujo"
local listados "raw/enumerators/Listados"
local reasignaciones "C:\Users\sarispe\Desktop"
local reasignaciones "`reasignaciones'\Matching Contributions"
local reasignaciones "`reasignaciones'\Listas encuestadores"
local reasignaciones "`reasignaciones'\Reasignaciones"

	import excel using ///
		"`enumerator'/190305--flujo de encuestadores.xlsx", ///
		firstrow  clear

	destring dni_encuestador , replace

	tostring Nro_de_cuenta Ruc , replace format("%15.0f")

	keep apellidopaterno - Ruc 

	gen DNI = string(dni_encuestador,"%08.0f")

	drop if username == ""

	foreach name of var apellidopaterno apellidomaterno ///
		primernombre Banco {

		replace `name' = upper(`name')
	}

	save "`enumerator'/190305--flujo de encuestadores.dta", ///
		replace


use "`project_data'", clear

keep if status!=.

drop if username=="estebanl" | username=="johanl" | ///
	username=="gabrielag" | username=="rossanag"

merge m:1 username using ///
	"`enumerator'/190305--flujo de encuestadores.dta", ///
	keepusing(username renuncia)

replace consent=. if consent==0

bys id consent: gen almenos1efectiva=_N
replace almenos1efectiva=. if consent==.

bys id: egen unaefectiva=total(almenos1efectiva)

preserve

keep if unaefectiva==1 & consent==1

gen Reasignacion="reasignacion"

*keep if fecha<td(01may2019)

keep Reasignacion username ruc id dist dir1_completa referencia interior ///
	razon_social nombre_comercial ciiu_descrip_inei strat_dist ///
	georeferencelatitude georeferencelongitude incidencias renuncia

tempfile empresaefectiva
save `empresaefectiva'

restore

drop if unaefectiva>0

gsort id fecha -starttime
bys id fecha: gen revisitinday=_n

keep if revisitinday==1

preserve

sort id fecha
bys id: gen last_status=status[_N]

replace last_status=0 if last_status!=1

keep if last_status==1

gsort id -fecha
bys id: gen last_status_close=_n

drop if last_status_close>1

gen Reasignacion="reasignacion"

*keep if fecha<td(01may2019)

keep Reasignacion username ruc id dist dir1_completa referencia interior ///
	razon_social nombre_comercial ciiu_descrip_inei strat_dist ///
	georeferencelatitude georeferencelongitude incidencias renuncia

tempfile empresacerrada
save `empresacerrada'

restore

sort id fecha
bys id: gen last_status=status[_N]

replace last_status=11 if last_status==0
replace last_status=0 if last_status!=11
replace last_status=1 if last_status==11

keep if last_status==1

sort id fecha
bys id: gen last_status2=status2[_N]

gen repro=(last_status2==1 | last_status2==-9999)
gen recha=last_status2==2

preserve

keep if repro==1

gsort id -fecha
bys id: gen last_status_repro=_n

drop if last_status_repro>1

gen Reasignacion="reasignacion"

*keep if fecha<td(01may2019)

keep Reasignacion username ruc id dist dir1_completa referencia interior ///
	razon_social nombre_comercial ciiu_descrip_inei strat_dist ///
	georeferencelatitude georeferencelongitude incidencias renuncia

tempfile empresarepro
save `empresarepro'

restore

keep if recha==1

gsort id -fecha
bys id: gen last_status_recha=_n

drop if last_status_recha>1

gen Reasignacion="reasignacion"

*keep if fecha<td(01may2019)

keep Reasignacion username ruc id dist dir1_completa referencia interior ///
	razon_social nombre_comercial ciiu_descrip_inei strat_dist ///
	georeferencelatitude georeferencelongitude incidencias renuncia

foreach i in empresarepro empresacerrada {
append using "``i''"
}

replace incidencias=upper(incidencias)

gen no_visitable=1 if strrpos(incidencias, "CONOCE")!=0

local x "CONOCE" "AFP" ///
	"NO UBICABLE" "NO DAN INFORMACION" "ADULTO MAYOR" ///
	"ADULTA MAYOR" "65" "YA NO ES EMPRESA" "NO VIVE" "NO LOCALIZADO" ///
	"ABSOLUTO" "ADULTOS MAYORES" "JUBILADO" "JUBILADA" ///
	"NO EXISTE" "FALLECI"	

foreach i in "`x'" {
replace no_visitable=1 if strrpos(incidencias, "`i'")!=0
}

drop if no_visitable==1

drop no_visitable

preserve

import excel "`reasignaciones'\Reasignaciones.xlsx", clear firstrow

bys id:gen dup=_n

drop if dup>1

drop dup

tempfile reasignacion

save `reasignacion'

restore

merge 1:1 id using "`reasignacion'"

keep if _merge==1

gen ncp_ciu=0
replace ncp_ciu=1 if strpos( ciiu_descrip_inei ,"n.c.p")
tab ncp_ciu

*Crear dummy de empresas que son n.c.p de otros servicios personales

gen ncp_ciu_servicios=0
replace ncp_ciu_servicios=1 if   ///
 strpos( ciiu_descrip_inei,"actividades de servicios personales n.c.p.")

*Crear dummy de empresas que son n.c.p de otros servicios empresariales
gen ncp_ciu_empresarial=0
replace ncp_ciu_empresarial=1 if   ///
 strpos( ciiu_descrip_inei,"servicios de apoyo a las empresas n.c.p.")

gen ncp=(ncp_ciu_empresarial==1)
 replace ncp=2 if ncp_ciu_servicios==1 & ncp==0
 replace ncp=3 if ncp_ciu==0

drop ncp_*

preserve

keep if renuncia==1

export excel using "`reasignaciones'\last_status_1.xlsx", first(var) replace

restore
preserve

keep if renuncia==0

export excel using "`reasignaciones'\last_status_0.xlsx", first(var) replace

restore

preserve

drop _merge

tempfile reasig 
save `reasig', replace

use "`listados'/Distribución TOTAL de listas.dta", clear

keep id Latitud_google Longitud_google

tempfile geos 
save `geos', replace

use "`reasig'", clear
merge 1:1 id using "`geos'"

keep if _merge!=2

replace georeferencelatitude=Latitud_google if georeferencelatitude==.
replace georeferencelongitude=Longitud_google if georeferencelongitude==.

marktouse nomissing georeferencelatitude georeferencelongitude

rename (georeferencelatitude georeferencelongitude) ///
	(latitud_google longitud_google)

export excel id dist latitud_google longitud_google strat_dist ///
	if nomissing==1 using "`rand_geo'/geo_reasignaciones.xlsx", first(var) replace

drop _merge

merge 1:1 id using "`rand_geo'/cluster_geos_ver7.dta",keepusing(id clust)

gen Encuestador_reasig=""

rename (latitud_google longitud_google) ///
	 (latitude longitude)

replace dist=upper(dist)
/*
export excel Encuestador_reasig ruc id	dist dir1_completa razon_social ///
	nombre_comercial interior referencia ciiu_descrip_inei strat_dist ///
	latitude longitude incidencias clust if nomissing==1 using ///
	"`listados'/Reasignaciones.xlsx", first(var) replace
*/

export excel Encuestador_reasig username ruc id	dist dir1_completa razon_social ///
	nombre_comercial interior referencia ciiu_descrip_inei strat_dist ///
	latitude longitude incidencias clust if nomissing==1 & username=="aracelyb" using ///
	"`listados'/Reasignaciones_AracelyB.xlsx", first(var) replace

restore
e
