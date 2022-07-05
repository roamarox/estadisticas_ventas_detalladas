

ejecutar_comando_sistema <- function(comando){
  comando <- unlist(strsplit(comando, split = " ", fixed = T))
  system2(comando[1], args = comando[-1])
}

subir_hdfs <- function(pais, mes, path){
  grilla <- Dimensiones::ObtenerGrillaGeoCanalDefault(pais , lista = T)
  
  lapply(grilla, function(g){
    flog.info('Subiendo %s, %s', g$geo, g$canal)
    canal_normalizado <- stringr::str_replace_all(g$canal, "\\+", "mas") %>% 
      stringr::str_replace_all(., " ", "_")
    geo_normalizado <- stringr::str_replace_all(g$geo, " ", "_")
    
    pattern <- sprintf('res_dia_hora_%s_%s', geo_normalizado, canal_normalizado)
    v <- lapply(list.files('.', pattern = pattern, full.names = T), function(archivo){
      fst::read_fst(archivo)
      v$cant_tickets_total_ <- NULL  
      v$cant_vta_total_ <- NULL
      v$imp_vta_total_ <- NULL
      v
    }) %>% rbindlist()
    
    file <- file.path(path, paste0(pattern, '.csv'))
    fwrite(v, file)
    
    hdfs <- sprintf('/u01/ds/datos/productos/estadisticas/%s/%s/%s', pais, mes, file)
    comando <- sprintf('hadoop fs -put -f %s %s', file, hdfs)
    ejecutar_comando_sistema(comando)
    unlink(file)
  })
  
  # 
  # lara <- fread('hadoop fs -text /u01/ds/datos/productos/estadisticas/br/202205/res_dia_hora_TOCANTINS_10mas.csv')
  # test <- v %>%
  #   filter(pdv_codigo == 11669) %>% 
  #   filter(categoria == 'Achocolatado em pó' | proveedor == 'NESTLE CIA') %>% 
  #   filter(hora == 15) %>%
  #   filter(dia == 1) 
  #   
  flog.info('FIN')
  
}
