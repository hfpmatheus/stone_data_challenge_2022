# Imports
import pandas          as pd
from datetime             import datetime


# Datasets
df_portfolio_geral = pd.read_csv( 'Arquivos.csv/portfolio_geral.csv' )
df_portfolio_clientes = pd.read_csv( 'Arquivos.csv/portfolio_clientes.csv' )
df_portfolio_comunicados = pd.read_csv( 'Arquivos.csv/portfolio_comunicados.csv' )
df_portfolio_tpv = pd.read_csv( 'Arquivos.csv/portfolio_tpv.csv' )

# Função de limpeza
def limpeza( portfolio_geral, portfolio_clientes, portfolio_comunicados, portfolio_tpv ):
    ### ============== Portfolio Geral ============== ###
    ### Alteração dos tipos dos Dados

    # dt_ref_portfolio
    portfolio_geral['dt_ref_portfolio']= pd.to_datetime( portfolio_geral['dt_ref_portfolio'] )

    # safra
    portfolio_geral['safra']= pd.to_datetime( portfolio_geral['safra'], format='%Y-%m' )

    # dt_contrato
    portfolio_geral['dt_contrato']= pd.to_datetime( portfolio_geral['dt_contrato'] )

    # dt_desembolso
    portfolio_geral['dt_desembolso']= pd.to_datetime( portfolio_geral['dt_desembolso'] )

    # dt_vencimento
    portfolio_geral['dt_vencimento']= pd.to_datetime( portfolio_geral['dt_vencimento'] )

    # dt_wo
    portfolio_geral['dt_wo']= pd.to_datetime( portfolio_geral['dt_wo'] )

    # prazo
    portfolio_geral['prazo'] = portfolio_geral['prazo'].astype( 'int64' )

    # corrigindo valores faltantes
    portfolio_geral['vlr_tarifa'] = portfolio_geral['vlr_desembolsado'].apply( lambda x: x*0.01 )
    
    ### ============== Portfolio Clientes ============== ###


    # dropando duplicados
    portfolio_clientes = portfolio_clientes.drop_duplicates(subset=['nr_documento'], keep='first')

    #### 28 UFs, deveriam ser 27.

    portfolio_clientes = portfolio_clientes.loc[ portfolio_clientes['cidade'] != 'N/D' ]
    

    ### ============== Portfolio Comunicados ============== ###
    ### Alteração dos tipos dos Dados

    # dt_ref_portfolio
    portfolio_comunicados['dt_ref_portfolio'] = pd.to_datetime( portfolio_comunicados['dt_ref_portfolio'] )

    # data_acao
    portfolio_comunicados['data_acao'] = pd.to_datetime( portfolio_comunicados['data_acao'] )
    
    ### ============== Portfolio TPV ============== ###
    # dt_transacao
    portfolio_tpv['dt_transacao'] = portfolio_tpv['dt_transacao'].apply(lambda x: pd.to_datetime(str(x), format='%Y%m%d'))
    
    ### ============== Portfolio Geral + TPV ============== ###
    ### Merge
    portfolio_geral_tpv = portfolio_geral.merge( portfolio_tpv, left_on=['nr_documento','dt_ref_portfolio'], right_on=['nr_documento','dt_transacao'], how='left' )

    #### Substituição de NA

    # posso dropar dt_transacao porque é a mesma coisa de dt_ref_portfolio
    portfolio_geral_tpv = portfolio_geral_tpv.drop( columns=['dt_transacao' ] )
    # substituir qtde transacoes e vlr tpv por 0 - se não houve transação, não houve valor transacionado também. Ou seja = 0
    portfolio_geral_tpv = portfolio_geral_tpv.fillna(0)

    ### Filtragem de dados
    #### Percentual de retenção máximo acima de 100%
    portfolio_geral_tpv[ 'perc_retencao' ] = portfolio_geral_tpv[ 'perc_retencao' ].apply( lambda x: x/10 )

    #### Valor mínimo do pagamento realizado negativo
    portfolio_geral_tpv = portfolio_geral_tpv.loc[portfolio_geral_tpv['vlr_pgto_realizado'] >= 0]

    #### Valor do saldo devedor máximo negativo ( A stone está devendo pro próprio cliente? Muito estranho )
    portfolio_geral_tpv = portfolio_geral_tpv.loc[ portfolio_geral_tpv['vlr_saldo_devedor'] >= 0 ]

    #### Verificar se existem dias com flag 1, porém tpv 0 ( não faz sentido, já que tpv 0 indica que não teve transação e flag 1 indica que sim )
    portfolio_geral_tpv['flag_transacao'] = portfolio_geral_tpv['vlr_tpv'].apply( lambda x: 1 if x > 0 else 0 )

    #### Quantidade mínima de transações negativa
    portfolio_geral_tpv = portfolio_geral_tpv.loc[ portfolio_geral_tpv['qtd_transacoes'] >= 0 ]

    #### Valor mínimo de tpv negativo
    portfolio_geral_tpv = portfolio_geral_tpv.loc[ portfolio_geral_tpv['vlr_tpv'] >= 0 ]
    
    # drop das linhas abaixo do primeiro status contrato 'settled' de cada contrato
    portfolio_aux_drop_settled_duplicates = portfolio_geral_tpv.loc[portfolio_geral_tpv['status_contrato'] =='Settled']
    portfolio_aux_drop_settled_duplicates = portfolio_aux_drop_settled_duplicates.sort_values( 'dt_ref_portfolio' ).drop_duplicates( subset=['contrato_id','status_contrato'], keep='first')

    # drop de todas as linhas 'settled' do dataset oringinal e merge com o dataset somente com os primeiros 'settled'
    portfolio_geral_tpv = portfolio_geral_tpv.loc[portfolio_geral_tpv['status_contrato'] != 'Settled' ]
    portfolio_geral_tpv = pd.concat( [ portfolio_geral_tpv, portfolio_aux_drop_settled_duplicates], axis=0 )
     
    return portfolio_clientes, portfolio_comunicados, portfolio_geral_tpv
     
print( 'Esse processo pode demorar aproximadamente 1 hora. Os datasets são muito grandes' )
     
# Limpando os dados com a função
portfolio_clientes_limpo, portfolio_comunicados_limpo, portfolio_geral_tpv = limpeza( df_portfolio_geral, df_portfolio_clientes, df_portfolio_comunicados, df_portfolio_tpv )
     
# Exportando datasets limpos pra csv 
portfolio_clientes_limpo.to_csv('Arquivos.csv/portfolio_clientes_limpo.csv')
portfolio_comunicados_limpo.to_csv('Arquivos.csv/portfolio_comunicados_limpo.csv')
portfolio_geral_tpv.to_csv('Arquivos.csv/portfolio_geral_tpv.csv')
     
print( 'Datasets exportados para a pasta Arquivos.csv com sucesso' )
