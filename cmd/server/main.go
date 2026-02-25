func main() {
    // 1. Conecta no Banco
    db := setupPostgres() 
    
    // 2. Instancia o Repository
    repo := repository.NewPostgresRepository(db)
    
    // 3. Instancia o Service passando o Repo
    service := services.NewPortService(repo)
    
    // 4. Instancia o Handler HTTP e o Consumidor Kafka passando o Service
    // ...
}