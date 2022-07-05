library(dplyr)
library(data.table)
library(futile.logger)
source('src/subir_hdfs.R')

pais <- 'br'
mes <- '202205'

productos <- Dimensiones::ObtenerProductos2(pais = pais, pemp_codigo = Dimensiones::EmpresaPrueba(pais), proyeccion = ' "Categoria", "Proveedor"')
setDT(productos)

geo_canal <- Dimensiones::ObtenerGrillaGeoCanalDefault(pais, lista = T)

lapply(geo_canal, function(gc){
  geo <- gc$geo
  canal <- gc$canal
  flog.info('Extrayendo ventas %s %s', geo, canal)
  canal_normalizado <- stringr::str_replace_all(canal, "\\+", "mas") %>% 
    stringr::str_replace_all(., " ", "_")
  geo_normalizado <- stringr::str_replace_all(geo, " ", "_")
  
  if(file.exists(sprintf('res_dia_hora_%s_%s.fst', geo_normalizado, canal_normalizado))){
    return(NULL)
  }
  ventas <- hechos::f_obtener_ventas_detalladas_por_geo_canal(pais= pais, geo = geo, canal = canal, mes = mes, incluir_venta_codigo_interno = F)
  setDT(ventas)
  Kcant_particiones <- 10
  
  flog.info('Joineando ventas %s %s', geo, canal)
  todos <- rje::powerSet(c('categoria', 'proveedor'), 8)
  
  
  ventas <- ventas[, c('dia', 'pdv_codigo_group') := .(format(fecha_comercial, '%u'), pdv_codigo %% Kcant_particiones)]
  
  ventas <- ventas %>%
    group_split(pdv_codigo_group)
  
  flog.info('Procesando ventas %s %s', geo, canal)
  j <<- 1
  lapply(ventas, function(v) {
    setDT(v)
    v <- v[productos, on = 'prod_codigo', nomatch = 0]
    
    flog.info('Procesando ventas %s %s iteracion %s', geo, canal, j)
    j <<- j + 1
    res <- lapply(todos, function(gb) {
      #Cuando el largo de gb es 0, no se agrupa por nada, no es un resultado que me interese
      if (length(gb) == 0) return(NULL)
      
      v[, setNames(.(sum(imp_vta), sum(cant_vta), uniqueN(id_ticket)), c(
        paste0('imp_vta_total', paste0(paste0('_', gb), collapse = '')),
        paste0('cant_vta_total', paste0(paste0('_', gb), collapse =
                                          '')),
        paste0('cant_tickets_total', paste0(paste0('_', gb), collapse =
                                              ''))
      )), by = c(gb, 'pdv_codigo', 'dia', 'hora')]
    })
    
    res <- rev(res) %>% 
      purrr::reduce(function(x, y){
        campos_comunes <- intersect(names(x), names(y))
        print(campos_comunes)
        merge(x, y, all = TRUE, by = campos_comunes)
      } )
    path <- file.path('datos', pais, mes)
    dir.create(path, recursive = T, showWarnings = F)
    fst::write.fst(res, sprintf('res_dia_hora_%s_%s_%s.fst', geo_normalizado, canal_normalizado, j))
  })
  NULL  
})

subir_hdfs(pais, mes, path)

