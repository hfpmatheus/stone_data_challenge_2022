# Imports
import pandas   as pd

# Datasets
df_portfolio_clientes = pd.read_csv( 'Arquivos.csv/portfolio_clientes_limpo.csv', index_col=0 )
df_portfolio_comunicados = pd.read_csv( 'Arquivos.csv/portfolio_comunicados_limpo.csv', index_col=0 )
df_portfolio_geral_tpv = pd.read_csv( 'Arquivos.csv/portfolio_geral_tpv.csv', index_col=0 )

# Função feature engineering
def feature_engineering( df_portfolio_clientes, df_portfolio_comunicados, df_portfolio_geral_tpv ):
    ### ============== Portfolio Clientes ============== ###
    # região

    norte = ['AC','AP','AM','PA','RO','RR','TO']
    nordeste = ['AL','BA','CE','MA','PB','PI','PE','RN','SE']
    centro_oeste = [ 'DF','GO','MT','MS' ]
    sudeste = ['ES','MG','RJ','SP']
    sul = ['PR','RS','SC']

    df_portfolio_clientes['regiao'] = df_portfolio_clientes['estado'].apply( lambda x: 'Norte' if x in norte else 'Nordeste' 
                                                                                               if x in nordeste else 'Centro_Oeste'
                                                                                               if x in centro_oeste else 'Sudeste'
                                                                                               if x in sudeste else 'Sul' )

    ### ============== Portfolio Comunicados ============== ###
    # negativado -> ( negativaçao - 1 ) / ( não negativação - 0 )
    df_negativados = df_portfolio_comunicados.loc[df_portfolio_comunicados['acao'] == 'campanhanegativacao' ]
    lista_negativados = df_negativados['contrato_id'].tolist()

    df_portfolio_comunicados['negativado'] = df_portfolio_comunicados['contrato_id'].apply( lambda x: 0 if x in lista_negativados else 1 )

    df_portfolio_comunicados.head()

    ### ============== Portfolio Geral + TPV ============== ###
    ## Convertendo colunas de data pra datetime pois quando se exporta pra csv elas voltam para float

    # dt_ref_portfolio
    df_portfolio_geral_tpv['dt_ref_portfolio']= pd.to_datetime( df_portfolio_geral_tpv['dt_ref_portfolio'] )

    # safra
    df_portfolio_geral_tpv['safra']= pd.to_datetime( df_portfolio_geral_tpv['safra'], format='%Y-%m' )

    # dt_contrato
    df_portfolio_geral_tpv['dt_contrato']= pd.to_datetime( df_portfolio_geral_tpv['dt_contrato'] )

    # dt_desembolso
    df_portfolio_geral_tpv['dt_desembolso']= pd.to_datetime( df_portfolio_geral_tpv['dt_desembolso'] )

    # dt_vencimento
    df_portfolio_geral_tpv['dt_vencimento']= pd.to_datetime( df_portfolio_geral_tpv['dt_vencimento'] )

    # dt_wo
    df_portfolio_geral_tpv['dt_wo']= pd.to_datetime( df_portfolio_geral_tpv['dt_wo'] )

    ## Features

    # settled (1) - not settled (0)
    df_settled = df_portfolio_geral_tpv.loc[df_portfolio_geral_tpv['status_contrato'] == 'Settled' ]
    settled_lista = df_settled['contrato_id'].tolist()

    df_portfolio_geral_tpv['settled'] = df_portfolio_geral_tpv['contrato_id'].apply( lambda x: 1 if x in settled_lista else 0 )

    # dt ref portfolio dia da semana
    df_portfolio_geral_tpv['dt_ref_portfolio_dia_da_semana'] = df_portfolio_geral_tpv['dt_ref_portfolio'].dt.dayofweek

    # dt ref portfolio dia
    df_portfolio_geral_tpv['dt_ref_portfolio_dia'] = df_portfolio_geral_tpv['dt_ref_portfolio'].dt.day

    # dt ref portfolio semana do ano
    df_portfolio_geral_tpv['dt_ref_portfolio_semana_do_ano'] = df_portfolio_geral_tpv['dt_ref_portfolio'].dt.weekofyear

    # dt ref portfolio mês
    df_portfolio_geral_tpv['dt_ref_portfolio_mes'] = df_portfolio_geral_tpv['dt_ref_portfolio'].dt.month

    # dt ref portfolio trimestre
    df_portfolio_geral_tpv['dt_ref_portfolio_trimestre'] = df_portfolio_geral_tpv['dt_ref_portfolio'].dt.quarter

    # dt ref portfolio ano
    df_portfolio_geral_tpv['dt_ref_portfolio_ano'] = df_portfolio_geral_tpv['dt_ref_portfolio'].dt.year
    
    return df_portfolio_clientes, df_portfolio_comunicados, df_portfolio_geral_tpv

print( 'Esse processo pode demorar um pouco. Os datasets são muito grandes' )
     
# Fazendo a feature engineering nos datasets com a função
portfolio_clientes_final, portfolio_comunicados_final, portfolio_geral_tpv_final = feature_engineering( df_portfolio_clientes, df_portfolio_comunicados, df_portfolio_geral_tpv )

# Exportando datasets com novas features pra csv
portfolio_clientes_final.to_csv('Arquivos.csv/portfolio_clientes_final.csv', index=False )
portfolio_comunicados_final.to_csv('Arquivos.csv/portfolio_comunicados_final.csv', index=False )
portfolio_geral_tpv_final.to_csv('Arquivos.csv/portfolio_geral_tpv_final.csv', index=False )
     
print( 'Datasets exportados para a pasta Arquivos.csv com sucesso.' )
