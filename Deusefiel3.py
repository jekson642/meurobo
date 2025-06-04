# -*- coding: utf-8 -*-
# ARQUIVO: Deusefiel_com_GA_corrigido.py (Streamlit com Gradiente Averaging)

import streamlit as st
import MetaTrader5 as mt5
import pandas as pd
import numpy as np
import datetime
import threading
import time
import logging
import os

# --- Configuração Inicial ---
LOG_FILE = "robo_streamlit_ga_log_corrigido.txt" # Nome do log alterado
MT5_MAGIC_NUMBER = 123457

if os.path.exists(LOG_FILE):
    try: os.remove(LOG_FILE)
    except Exception as e: print(f"Erro ao remover log antigo: {e}")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(threadName)s: %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
    ]
)
logger = logging.getLogger(__name__)

IS_ON_STREAMLIT_CLOUD = os.getenv("IS_STREAMLIT_CLOUD", "false").lower() == "true"

# --- Estado da Aplicação ---
def inicializar_estado_sessao_p3_completo_com_ga():
    ss = st.session_state
    if "app_p3_ga_initialized_v1_corrigido" not in ss: # Chave de inicialização atualizada
        logger.info("Inicializando estado da sessão Streamlit (com Gradiente Averaging - Corrigido).")
        if IS_ON_STREAMLIT_CLOUD:
            logger.warning("MODO STREAMLIT CLOUD ATIVADO - Funcionalidades MT5 Live/Dados serão limitadas.")

        ss.robo_esta_ligado = False
        ss.mt5_esta_conectado = False
        ss.status_interface = "Robô Desligado"
        ss.conexao_mt5_interface = "MT5 Desabilitado (Cloud)" if IS_ON_STREAMLIT_CLOUD else "MT5 Desconectado"
        ss.mt5_login_input = ""
        ss.mt5_password_input = ""
        ss.mt5_server_input = ""
        ss.logs_para_display = [f"Log da aplicação (com GA - Corrigido) inicializado. Cloud: {IS_ON_STREAMLIT_CLOUD}"]
        ss.robot_thread_referencia = None
        ss.stop_robot_event_obj = threading.Event()
        ss.robot_thread_deve_rodar = False
        ss.ativo_principal_live = "WINDFUT"; ss.timeframe_live = 5; ss.volume_live = 1.0
        ss.stop_loss_pts_live = 100.0; ss.stop_win_pts_live = 200.0
        ss.tenkan_period_live = 9; ss.kijun_period_live = 26; ss.senkou_b_period_live = 52
        ss.hora_inicio_live = "09:00"; ss.hora_fim_live = "17:30"; ss.delay_entry_points_live = 0.0 # Corrigido para float
        ss.max_loss_trade_financeiro_live = 50.0
        ss.daily_total_stop_loss_financeiro_live = 200.0
        ss.posicao_info_live = "Posição Robô: N/A"; ss.pnl_info_live = "PNL Dia: -- / PNL Op.: --"
        ss.tick_price_display_live = "Preço: N/A"; ss.pnl_realizado_hoje_live = 0.0
        ss.data_ultimo_pnl_reset_live = datetime.date.min; ss.stop_diario_atingido_hoje_live = False
        ss.processed_deal_tickets_live = set()
        ss.last_history_check_time_live = datetime.datetime.now() - datetime.timedelta(minutes=60)
        ss.pending_signal_live = None; ss.intervalo_robo_segundos = 5
        ss.ga_live_ativo_checkbox = False
        ss.ga_live_ciclo_ativo = False
        ss.ga_live_ciclo_info = {}
        ss.ga_live_ciclo_posicoes_ids = []
        ss.ga_live_ciclo_nivel_atual = 0
        ss.ga_live_ciclo_proximo_nivel_preco_ativacao = 0.0
        ss.ga_live_dist_base_pts = 100
        ss.ga_live_pnl_sw_ciclo_pts = 150
        ss.ga_live_vol_inicial = 1.0
        ss.ga_live_vol_nivel_sub = 1.0
        ss.ga_live_max_niveis = 5
        ss.pending_signal_ga_live = None
        ss.bt_ativo = "WINDFUT"
        ss.bt_start_date = datetime.date.today() - datetime.timedelta(days=30)
        ss.bt_end_date = datetime.date.today() - datetime.timedelta(days=1)
        ss.bt_timeframe = 5; ss.bt_initial_balance = 10000.0; ss.bt_volume = 1.0
        ss.bt_sl_pts = 100.0; ss.bt_tp_pts = 200.0; ss.bt_delay_entry_pts = 0.0 # Corrigido para float
        ss.bt_tenkan_period = 9; ss.bt_kijun_period = 26; ss.bt_senkou_b_period = 52
        ss.bt_hora_inicio_op = "09:00"; ss.bt_hora_fim_op = "17:30"
        ss.bt_stop_financeiro_trade = 0.0; ss.bt_stop_diario_total = 0.0
        ss.bt_fechar_pos_stop_diario = True
        ss.bt_thread_referencia = None; ss.bt_esta_rodando = False; ss.bt_progresso = 0.0
        ss.bt_trades_realizados_df = pd.DataFrame()
        ss.bt_equity_curve_df = pd.DataFrame(columns=['time', 'balance'])
        ss.bt_pnl_total_str = "PNL Total: R$ 0.00"; ss.bt_num_trades_str = "Trades: 0"
        ss.bt_win_rate_str = "Win Rate: 0.00%"
        ss.bt_status_message = "Pronto para iniciar BT."
        if IS_ON_STREAMLIT_CLOUD: ss.bt_status_message = "BT (Demonstração - Sem dados MT5 reais)."
        ss.bt_usar_ga_checkbox = False
        ss.bt_ga_ciclo_ativo = False
        ss.bt_ga_ciclo_info = {}
        ss.bt_ga_dist_base_pts = 100
        ss.bt_ga_pnl_sw_ciclo_pts = 150
        ss.bt_ga_vol_inicial = 1.0
        ss.bt_ga_vol_nivel_sub = 1.0
        ss.bt_ga_max_niveis = 5
        ss.app_p3_ga_initialized_v1_corrigido = True # Chave de inicialização atualizada
        logger.info("Estado da sessão (com GA - Corrigido) totalmente inicializado.")

inicializar_estado_sessao_p3_completo_com_ga()

# --- Funções de Logging ---
def log_st_e_arquivo(message, level="INFO"):
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    log_entry_display = f"{timestamp} [{level.upper()}] {message}"
    if level.upper() == "DEBUG": logger.debug(message)
    elif level.upper() == "INFO": logger.info(message)
    elif level.upper() == "WARNING": logger.warning(message)
    elif level.upper() == "ERROR": logger.error(message)
    elif level.upper() == "CRITICAL": logger.critical(message)
    if "logs_para_display" in st.session_state:
        st.session_state.logs_para_display.append(log_entry_display)
        if len(st.session_state.logs_para_display) > 200:
            st.session_state.logs_para_display.pop(0)

# --- Funções de Conexão MT5 ---
def conectar_mt5_st(login_str, password_str, server_str):
    log_st_e_arquivo("Tentando conectar ao MetaTrader 5...", "INFO")
    st.session_state.conexao_mt5_interface = "MT5 Conectando..."
    st.session_state.mt5_esta_conectado = False
    initialized_successfully_flag = False
    if IS_ON_STREAMLIT_CLOUD:
        log_st_e_arquivo("Ambiente Streamlit Cloud detectado. Conexão MT5 real não é possível.", "WARN")
        st.session_state.conexao_mt5_interface = "MT5 Desabilitado (Cloud)"
        return False
    params = {}
    try:
        login_int = int(login_str.strip())
        params['login'] = login_int
    except ValueError:
        if login_str and login_str.strip():
             log_st_e_arquivo(f"Login MT5 '{login_str}' não numérico. Tentando sem.", "WARN")
    if password_str and password_str.strip(): params['password'] = password_str.strip()
    if server_str and server_str.strip(): params['server'] = server_str.strip()
    try:
        current_terminal_info = None
        try:
            current_terminal_info = mt5.terminal_info()
        except Exception: # Não falhar se terminal_info() der erro antes de initialize
            pass
        if current_terminal_info is not None:
            log_st_e_arquivo("MT5 já parece inicializado. Tentando desligar antes de reconectar.", "DEBUG")
            try:
                mt5.shutdown()
                time.sleep(0.2)
            except Exception as e_sd_prev:
                log_st_e_arquivo(f"Exceção ao tentar desligar MT5 pré-existente: {e_sd_prev}", "WARN")

        if not mt5.initialize(**params, timeout=7000):
            err_code, err_msg = 0, "N/A"
            try:
                error_tuple_init = mt5.last_error()
                if error_tuple_init:
                    err_code = error_tuple_init[0]
                    err_msg = error_tuple_init[1]
            except Exception as e_le_init:
                log_st_e_arquivo(f"Exceção ao chamar mt5.last_error() em initialize: {e_le_init}", "WARN")

            log_st_e_arquivo(f"MT5 initialize() falhou. Cód: {err_code}, Msg: {err_msg}", "ERROR")
            st.session_state.conexao_mt5_interface = f"MT5 Falha Ini. ({err_code})"
        else:
            initialized_successfully_flag = True
            ti = None; ai = None
            try:
                ti = mt5.terminal_info()
                ai = mt5.account_info()
            except Exception as e_info:
                log_st_e_arquivo(f"Exceção ao obter terminal_info/account_info: {e_info}", "ERROR")

            if ti and ai:
                log_st_e_arquivo(f"MT5 conectado. Servidor: {ai.server}, Login: {ai.login}", "INFO")
                st.session_state.mt5_esta_conectado = True
                st.session_state.conexao_mt5_interface = f"MT5 Conectado (Login: {ai.login})"
            else:
                log_st_e_arquivo("MT5 inicializado, mas falha ao obter info (terminal/account).", "ERROR")
                st.session_state.conexao_mt5_interface = "MT5 Conectado (Info Falhou)"
                if initialized_successfully_flag:
                    try:
                        mt5.shutdown()
                    except Exception as e_sd_info_fail:
                        log_st_e_arquivo(f"Exceção ao desligar MT5 após falha de info: {e_sd_info_fail}", "WARN")
                st.session_state.mt5_esta_conectado = False # Garantir que o estado reflita a falha
    except Exception as e:
        log_st_e_arquivo(f"Exceção GERAL na tentativa de inicialização MT5: {e}", "CRITICAL")
        st.session_state.conexao_mt5_interface = "MT5 Erro Exceção Grave"
        if initialized_successfully_flag and hasattr(mt5, 'shutdown'): # Checa se mt5 foi importado e tem shutdown
            try:
                log_st_e_arquivo(f"Tentando mt5.shutdown() após exceção geral...", "DEBUG")
                mt5.shutdown()
            except Exception as e_sd_fatal:
                log_st_e_arquivo(f"Exceção ao tentar mt5.shutdown() após erro grave: {e_sd_fatal}", "ERROR")
        st.session_state.mt5_esta_conectado = False
    return st.session_state.mt5_esta_conectado

def desligar_mt5_st():
    if IS_ON_STREAMLIT_CLOUD:
        log_st_e_arquivo("Ambiente Streamlit Cloud. Desligamento MT5 não aplicável/necessário.", "DEBUG")
        st.session_state.mt5_esta_conectado = False
        st.session_state.conexao_mt5_interface = "MT5 Desabilitado (Cloud)"
        return

    can_try_shutdown = False
    try:
        # Verifica se o MT5 está minimamente utilizável para chamar terminal_info
        if mt5.terminal_info() is not None: # Isso já implica que mt5 foi importado e initialized (ou tentou ser)
            can_try_shutdown = True
    except Exception: # Se terminal_info() falhar (e.g. DLL não carregada), não tentar shutdown.
        pass

    if can_try_shutdown:
        log_st_e_arquivo("Desligando conexão com MetaTrader 5...", "INFO")
        try:
            mt5.shutdown()
        except Exception as e_sd:
            log_st_e_arquivo(f"Exceção durante mt5.shutdown(): {e_sd}", "ERROR")
    else:
        log_st_e_arquivo("MT5 já desligado, não inicializado, ou em estado instável para desligar.", "DEBUG")

    st.session_state.mt5_esta_conectado = False
    st.session_state.conexao_mt5_interface = "MT5 Desconectado (Manual)"

# --- Funções de Cálculo e Dados ---
def calcular_ichimoku_st(df_input, tenkan_period=9, kijun_period=26, senkou_b_period=52):
    if df_input is None or df_input.empty: return pd.DataFrame()
    df = df_input.copy()
    min_data = max(tenkan_period, kijun_period, senkou_b_period) + kijun_period
    if len(df) < min_data:
        log_st_e_arquivo(f"Ichimoku: Dados insuficientes ({len(df)} de {min_data} necessários).", "WARN")
        cols = ['tenkan_sen', 'kijun_sen', 'senkou_span_a_calc', 'senkou_span_b_calc',
                'chikou_span_data', 'senkou_span_a_plot', 'senkou_span_b_plot', 'chikou_span_plot']
        for col in cols: df[col] = np.nan
        return df
    df['tenkan_sen'] = (df['high'].rolling(window=tenkan_period, min_periods=1).max() + df['low'].rolling(window=tenkan_period, min_periods=1).min()) / 2
    df['kijun_sen'] = (df['high'].rolling(window=kijun_period, min_periods=1).max() + df['low'].rolling(window=kijun_period, min_periods=1).min()) / 2
    df['senkou_span_a_calc'] = ((df['tenkan_sen'] + df['kijun_sen']) / 2)
    df['senkou_span_a_plot'] = df['senkou_span_a_calc'].shift(kijun_period)
    df['senkou_span_b_calc'] = (df['high'].rolling(window=senkou_b_period, min_periods=1).max() + df['low'].rolling(window=senkou_b_period, min_periods=1).min()) / 2
    df['senkou_span_b_plot'] = df['senkou_span_b_calc'].shift(kijun_period)
    # Chikou Span (Lagging Span): Current close plotted 'kijun_period' bars in the past.
    # 'chikou_span_data' is the actual close price for the current bar.
    df['chikou_span_data'] = df['close']
    # 'chikou_span_plot' is this data shifted for plotting.
    df['chikou_span_plot'] = df['close'].shift(-kijun_period)
    return df

def obter_dados_mt5_st(symbol, timeframe_minutes, n_candles=200):
    ss = st.session_state
    if IS_ON_STREAMLIT_CLOUD:
        log_st_e_arquivo(f"Obter Dados Live: Não funcional no Streamlit Cloud para {symbol}.", "WARN")
        return None
    if not ss.get("mt5_esta_conectado", False):
        log_st_e_arquivo(f"Obter Dados Live: MT5 não conectado para {symbol}.", "WARN")
        return None

    tf_map = {1: mt5.TIMEFRAME_M1, 5: mt5.TIMEFRAME_M5, 15: mt5.TIMEFRAME_M15, 30: mt5.TIMEFRAME_M30,
              60: mt5.TIMEFRAME_H1, 240: mt5.TIMEFRAME_H4, 1440: mt5.TIMEFRAME_D1}
    mt5tf = tf_map.get(timeframe_minutes, mt5.TIMEFRAME_M1)

    s_info = None
    try:
        s_info = mt5.symbol_info(symbol)
    except Exception as e_si:
        log_st_e_arquivo(f"Dados: Exceção ao obter symbol_info para {symbol}: {e_si}", "ERROR")
        return None

    if s_info is None:
        log_st_e_arquivo(f"Dados: Símbolo {symbol} não encontrado no MT5.", "ERROR")
        return None
    if not s_info.visible:
        log_st_e_arquivo(f"Dados: {symbol} não visível, tentando selecionar...", "INFO")
        selected = False
        try:
            selected = mt5.symbol_select(symbol, True)
        except Exception as e_ss:
            log_st_e_arquivo(f"Dados: Exceção ao chamar symbol_select para {symbol}: {e_ss}", "ERROR")

        if not selected:
            err_code_sel, err_msg_sel = 0, "N/A"
            try:
                err_tuple_sel = mt5.last_error()
                if err_tuple_sel: err_code_sel, err_msg_sel = err_tuple_sel
            except Exception as e_le_sel:
                log_st_e_arquivo(f"Exceção mt5.last_error() em symbol_select: {e_le_sel}", "WARN")
            log_st_e_arquivo(f"Dados: Falha ao tornar {symbol} visível. Erro MT5 Cód: {err_code_sel}, Msg: {err_msg_sel}", "ERROR")
            return None
        time.sleep(0.3) # Give time for symbol to be available

    # Determine required candles for Ichimoku based on live periods
    # These periods should come from st.session_state for live data
    tenkan_p = ss.get('tenkan_period_live', 9)
    kijun_p = ss.get('kijun_period_live', 26)
    senkou_b_p = ss.get('senkou_b_period_live', 52)

    n_req_ichi = max(tenkan_p, kijun_p, senkou_b_p) + kijun_p
    n_req_final = max(n_req_ichi, n_candles) + 50 # Extra buffer

    rates = None
    try:
        rates = mt5.copy_rates_from_pos(symbol, mt5tf, 0, n_req_final)
    except Exception as e_cr:
        log_st_e_arquivo(f"Dados: Exceção ao obter rates para {symbol}: {e_cr}", "ERROR")
        return None

    if rates is None or len(rates) == 0:
        err_code_rates, err_msg_rates = 0, "N/A"
        try:
            error_tuple_rates = mt5.last_error()
            if error_tuple_rates:
                err_code_rates = error_tuple_rates[0]
                err_msg_rates = error_tuple_rates[1]
        except Exception as e_le_rates:
            log_st_e_arquivo(f"Exceção ao chamar mt5.last_error() em copy_rates: {e_le_rates}", "WARN")
        log_st_e_arquivo(f"Dados: Nenhum dado retornado para {symbol} (TF {timeframe_minutes}m). Erro MT5 Cód: {err_code_rates}, Msg: {err_msg_rates}", "WARN")
        return None

    df = pd.DataFrame(rates); df['time'] = pd.to_datetime(df['time'], unit='s'); df.set_index('time', inplace=True); df.sort_index(inplace=True)
    return df

def validar_volume_st(symbol, volume):
    ss = st.session_state
    if IS_ON_STREAMLIT_CLOUD: return volume # Cannot validate without MT5
    if not ss.get("mt5_esta_conectado", False): return 0.0 # Cannot validate

    info = None
    try:
        info = mt5.symbol_info(symbol)
    except Exception as e_vi:
        log_st_e_arquivo(f"Volume: Exceção ao obter symbol_info para {symbol}: {e_vi}", "ERROR")
        return 0.0 # Cannot validate, assume invalid

    if not info: # Handles both s_info is None and exception case
        log_st_e_arquivo(f"Volume: Info não encontrada para {symbol}, não foi possível validar o volume.", "ERROR")
        return 0.0 # Cannot validate, assume invalid

    min_v, max_v, step_v = info.volume_min, info.volume_max, info.volume_step

    # Clamp to min/max
    volume = max(min_v, volume)
    volume = min(max_v, volume)

    # Adjust to step
    if step_v > 0:
        # Calculate how many steps fit into the volume relative to min_v (if volume starts from min_v)
        # Or more simply, round to the nearest step
        volume = round(volume / step_v) * step_v

        # Re-clamp after step rounding as it might go slightly out of bounds
        volume = max(min_v, volume)
        volume = min(max_v, volume)

        # Round to appropriate number of decimal places based on step_v
        num_dec = 0
        str_step_fmt = f"{step_v:.10f}".rstrip('0')
        if '.' in str_step_fmt:
            num_dec = len(str_step_fmt.split('.')[1])
        volume = round(volume, num_dec)

    if volume == 0 and min_v > 0: # If somehow volume ended up 0 but min_v is > 0
        return min_v
    return volume

# --- Lógica de Trading e Gerenciamento LIVE ---
def verificar_horario_operacao_st():
    ss = st.session_state
    try:
        h_ini = datetime.datetime.strptime(ss.hora_inicio_live, "%H:%M").time()
        h_fim = datetime.datetime.strptime(ss.hora_fim_live, "%H:%M").time()
    except ValueError:
        log_st_e_arquivo(f"Horário live inválido: {ss.hora_inicio_live} ou {ss.hora_fim_live}. Operações permitidas por default.", "ERROR")
        return True # Default to allow if format is wrong
    agora = datetime.datetime.now().time()
    if h_ini <= h_fim:
        return h_ini <= agora <= h_fim
    else: # Horário vira a noite (ex: 22:00 - 02:00)
        return agora >= h_ini or agora <= h_fim

def atualizar_pnl_diario_historico_st():
    ss = st.session_state; hoje = datetime.date.today()
    if ss.data_ultimo_pnl_reset_live != hoje:
        log_st_e_arquivo(f"Novo dia ({hoje}). Resetando PNL live e stop.", "INFO")
        ss.pnl_realizado_hoje_live = 0.0; ss.data_ultimo_pnl_reset_live = hoje
        ss.stop_diario_atingido_hoje_live = False; ss.processed_deal_tickets_live.clear()
        if 'pnl_realizado_no_ciclo' in ss.ga_live_ciclo_info: # Reset PNL do ciclo GA também se o dia mudou
            ss.ga_live_ciclo_info['pnl_realizado_no_ciclo'] = 0.0

    if IS_ON_STREAMLIT_CLOUD or not ss.get("mt5_esta_conectado", False): return
    # Rate limit history check
    if (datetime.datetime.now() - ss.last_history_check_time_live).total_seconds() < 45: return
    ss.last_history_check_time_live = datetime.datetime.now()

    from_dt = datetime.datetime.combine(hoje, datetime.time.min)
    to_dt = datetime.datetime.now() # Até o momento atual

    deals = None
    try:
        deals = mt5.history_deals_get(from_dt, to_dt)
    except Exception as e:
        log_st_e_arquivo(f"Erro ao buscar deals para PNL: {e}", "ERROR")
        return

    if deals is None:
        err_code_deals, err_msg_deals = 0, "N/A"
        try:
            error_tuple_deals = mt5.last_error()
            if error_tuple_deals:
                err_code_deals = error_tuple_deals[0]
                err_msg_deals = error_tuple_deals[1]
        except Exception as e_le_deals:
            log_st_e_arquivo(f"Exceção ao chamar mt5.last_error() em history_deals: {e_le_deals}", "WARN")
        log_st_e_arquivo(f"Falha ao obter deals para PNL. Erro MT5 Cód: {err_code_deals}, Msg: {err_msg_deals}", "WARN")
        return

    # PNL total dos novos deals processados
    current_day_pnl_from_new_deals = 0.0
    newly_processed_tickets_in_this_run = set()
    novos_deals_contados = 0

    for deal in deals:
        is_exit_type = deal.entry == mt5.DEAL_ENTRY_OUT or deal.entry == mt5.DEAL_ENTRY_INOUT
        # Only consider deals with our magic number and that are exits/closings
        if deal.magic == MT5_MAGIC_NUMBER and is_exit_type:
            if deal.ticket not in ss.processed_deal_tickets_live:
                current_day_pnl_from_new_deals += deal.profit
                newly_processed_tickets_in_this_run.add(deal.ticket)
                novos_deals_contados += 1

    if novos_deals_contados > 0:
        ss.pnl_realizado_hoje_live += current_day_pnl_from_new_deals # Add new PNL to existing daily PNL
        ss.processed_deal_tickets_live.update(newly_processed_tickets_in_this_run) # Mark as processed
        log_st_e_arquivo(f"PNL diário live atualizado: R${ss.pnl_realizado_hoje_live:.2f} (+R${current_day_pnl_from_new_deals:.2f} de {novos_deals_contados} novos deals).", "INFO")

    stop_configurado = ss.daily_total_stop_loss_financeiro_live
    if stop_configurado > 0 and ss.pnl_realizado_hoje_live <= -stop_configurado and not ss.stop_diario_atingido_hoje_live:
        log_st_e_arquivo(f"STOP DIÁRIO FINANCEIRO LIVE ATINGIDO! PNL: R${ss.pnl_realizado_hoje_live:.2f}, Limite: R${-stop_configurado:.2f}", "CRITICAL")
        ss.stop_diario_atingido_hoje_live = True
        if ss.ga_live_ativo_checkbox and ss.ga_live_ciclo_ativo: # Checkbox for GA might be off, but cycle could exist
            log_st_e_arquivo("Stop Diário Atingido: Fechando ciclo GA ativo.", "WARN")
            _st_fechar_posicoes_ciclo_ga_live(ss.ativo_principal_live, "Stop Diario Financeiro")

def atualizar_display_info_robo_st():
    ss = st.session_state
    if IS_ON_STREAMLIT_CLOUD or not ss.get("mt5_esta_conectado", False):
        ss.posicao_info_live = "Pos. Robô: MT5 Desc."
        ss.pnl_info_live = "PNL Dia: -- / PNL Op.: MT5 Desc."
        ss.tick_price_display_live = f"Preço ({ss.ativo_principal_live}): MT5 Desc."
        return

    symbol = ss.ativo_principal_live
    pnl_aberto_total_normal = 0.0
    pos_details_list_normal = []

    if ss.ga_live_ativo_checkbox and ss.ga_live_ciclo_ativo:
        _st_atualizar_info_ciclo_ga_live(symbol)

    pnl_aberto_total_ga = 0.0
    if ss.ga_live_ativo_checkbox and ss.ga_live_ciclo_ativo:
         pnl_aberto_total_ga = ss.ga_live_ciclo_info.get('pnl_flutuante_total', 0.0)

    positions = None
    try:
        positions = mt5.positions_get(symbol=symbol, magic=MT5_MAGIC_NUMBER)
    except Exception as e_pg:
        log_st_e_arquivo(f"Display Info: Exceção ao obter positions_get: {e_pg}", "ERROR")
        ss.posicao_info_live = "Pos. Robô: Erro ao buscar"
        ss.pnl_info_live = "PNL Dia: -- / PNL Op.: Erro ao buscar"

    s_info_display = None
    try:
        s_info_display = mt5.symbol_info(symbol)
    except:
        pass

    default_digits = s_info_display.digits if s_info_display else 2

    if positions:
        for pos in positions:
            is_ga_position = False
            if ss.ga_live_ativo_checkbox and ss.ga_live_ciclo_ativo and \
               ss.ga_live_ciclo_info.get('symbol') == pos.symbol and \
               pos.ticket in ss.ga_live_ciclo_posicoes_ids:
                is_ga_position = True

            if not is_ga_position:
                pnl_aberto_total_normal += pos.profit
                direction = "C" if pos.type == mt5.ORDER_TYPE_BUY else "V"
                pos_details_list_normal.append(f"{direction} {pos.volume:.2f}@{pos.price_open:.{default_digits}f}")

    if ss.ga_live_ativo_checkbox and ss.ga_live_ciclo_ativo:
        tipo_base_ga = ss.ga_live_ciclo_info.get('tipo_ordem_base', 'N/A')
        nivel_ga = ss.ga_live_ciclo_nivel_atual
        vol_total_ga = ss.ga_live_ciclo_info.get('volume_total_aberto', 0.0)
        pm_ga = ss.ga_live_ciclo_info.get('preco_medio_ponderado', 0.0)

        ss.posicao_info_live = f"GA {tipo_base_ga} Nv.{nivel_ga}: {vol_total_ga:.2f}@{pm_ga:.{default_digits}f}"
        pnl_total_ciclo_display = ss.ga_live_ciclo_info.get('pnl_realizado_no_ciclo', 0.0) + pnl_aberto_total_ga
        pnl_op_str = f"PNL Ciclo GA: R${pnl_total_ciclo_display:.2f}"
    else:
        ss.posicao_info_live = f"Pos. Normal: {', '.join(pos_details_list_normal) if pos_details_list_normal else 'Nenhuma'}"
        pnl_op_str = f"PNL Normal Aberto: R${pnl_aberto_total_normal:.2f}"

    pnl_dia_formatado = f"R${ss.pnl_realizado_hoje_live:.2f}"
    ss.pnl_info_live = f"PNL Dia: {pnl_dia_formatado} / {pnl_op_str}"

    tick = None
    try:
        tick = mt5.symbol_info_tick(symbol)
    except Exception as e_tick:
        log_st_e_arquivo(f"Display Info: Exceção ao obter tick para {symbol}: {e_tick}", "WARN")

    if tick and s_info_display:
        ss.tick_price_display_live = f"{symbol}: Bid {tick.bid:.{s_info_display.digits}f} / Ask {tick.ask:.{s_info_display.digits}f}"
    elif tick:
        ss.tick_price_display_live = f"{symbol}: Bid {tick.bid} / Ask {tick.ask}"
    else:
        ss.tick_price_display_live = f"Preço ({symbol}): --"

def gerar_sinal_ichimoku_st(df_data, ichimoku_params):
    ss = st.session_state

    required_cols = ['high', 'low', 'close', 'tenkan_sen', 'kijun_sen',
                     'senkou_span_a_plot', 'senkou_span_b_plot']

    kijun_period_val = ichimoku_params.get('kijun_period', 26)
    min_data_needed_for_signal = max(
        ichimoku_params.get('tenkan_period', 9),
        kijun_period_val,
        ichimoku_params.get('senkou_b_period', 52)
    ) + kijun_period_val + 2

    if df_data is None or df_data.empty or len(df_data) < min_data_needed_for_signal:
        return None

    if not all(col in df_data.columns for col in required_cols):
        log_st_e_arquivo("Sinal Ichimoku Live: Colunas essenciais ausentes no DataFrame.", "WARN")
        return None

    last = df_data.iloc[-1]
    prev = df_data.iloc[-2]

    for candle_ref_idx in [-1, -2]:
        candle_ref = df_data.iloc[candle_ref_idx]
        for col in required_cols:
            if pd.isna(candle_ref[col]):
                return None

    close = last['close']
    ssa = last['senkou_span_a_plot']
    ssb = last['senkou_span_b_plot']

    chikou_span_value = last['close']
    price_kijun_ago_high = df_data['high'].iloc[-1 - kijun_period_val]
    price_kijun_ago_low  = df_data['low'].iloc[-1 - kijun_period_val]

    cond_chikou_b = False
    if pd.notna(chikou_span_value) and pd.notna(price_kijun_ago_high):
        cond_chikou_b = chikou_span_value > price_kijun_ago_high

    cond_chikou_s = False
    if pd.notna(chikou_span_value) and pd.notna(price_kijun_ago_low):
        cond_chikou_s = chikou_span_value < price_kijun_ago_low

    tk_cross_buy = (prev['tenkan_sen'] < prev['kijun_sen']) and \
                   (last['tenkan_sen'] > last['kijun_sen'])
    tk_cross_sell = (prev['tenkan_sen'] > prev['kijun_sen']) and \
                    (last['tenkan_sen'] < last['kijun_sen'])

    price_abv_kumo = False
    if pd.notna(ssa) and pd.notna(ssb) and pd.notna(close):
        price_abv_kumo = close > max(ssa, ssb)

    price_blw_kumo = False
    if pd.notna(ssa) and pd.notna(ssb) and pd.notna(close):
        price_blw_kumo = close < min(ssa, ssb)

    signal_price_ref = last['close']

    if tk_cross_buy and price_abv_kumo and cond_chikou_b:
        return {'type': 'BUY', 'price_signal': signal_price_ref}

    if tk_cross_sell and price_blw_kumo and cond_chikou_s:
        return {'type': 'SELL', 'price_signal': signal_price_ref}

    return None

def processar_sinal_pendente_st():
    ss = st.session_state
    if not ss.pending_signal_live or IS_ON_STREAMLIT_CLOUD or not ss.get("mt5_esta_conectado", False):
        if ss.pending_signal_live and (IS_ON_STREAMLIT_CLOUD or not ss.get("mt5_esta_conectado", False)):
            log_st_e_arquivo(f"Processar Sinal Pendente Normal: Ignorado (Cloud={IS_ON_STREAMLIT_CLOUD}, MT5Conn={ss.get('mt5_esta_conectado', False)}).", "DEBUG")
        ss.pending_signal_live = None
        return

    sinal_normal = ss.pending_signal_live.copy()
    ativo = ss.ativo_principal_live
    log_st_e_arquivo(f"Processando Sinal Pendente Normal: {sinal_normal['type']} em {ativo} @ Sinal {sinal_normal['price_signal']}", "INFO")

    if ss.stop_diario_atingido_hoje_live:
        log_st_e_arquivo("Processar Sinal Normal Cancelado: Stop Diário Atingido.", "WARN")
        ss.pending_signal_live = None; return

    current_positions_check = None
    try:
        current_positions_check = mt5.positions_get(symbol=ativo, magic=MT5_MAGIC_NUMBER)
    except Exception as e_pc:
        log_st_e_arquivo(f"Processar Sinal Normal: Erro ao verificar posições existentes: {e_pc}", "ERROR")
        return

    if current_positions_check and len(current_positions_check) > 0:
        log_st_e_arquivo(f"Processar Sinal Normal: Já existe(m) {len(current_positions_check)} posição(ões) para {ativo} (Magic: {MT5_MAGIC_NUMBER}). Sinal ignorado.", "INFO")
        ss.pending_signal_live = None
        return

    vol_validado = validar_volume_st(ativo, ss.volume_live)
    if vol_validado == 0.0:
        log_st_e_arquivo(f"Processar Sinal Normal: Volume {ss.volume_live} resultou em {vol_validado} (inválido) para {ativo}. Sinal ignorado.", "ERROR")
        ss.pending_signal_live = None; return

    s_info = None; tick = None
    try:
        s_info = mt5.symbol_info(ativo)
        tick = mt5.symbol_info_tick(ativo)
    except Exception as e_si_ti:
        log_st_e_arquivo(f"Processar Sinal Normal: Erro ao obter info/tick para {ativo}: {e_si_ti}", "ERROR")
        return

    if not s_info or not tick:
        log_st_e_arquivo(f"Processar Sinal Normal: Falha ao obter info/tick para {ativo}. Sinal ignorado.", "ERROR")
        return

    point, digits = s_info.point, s_info.digits
    preco_sinal_original = sinal_normal['price_signal']

    execution_price = tick.ask if sinal_normal['type'] == 'BUY' else tick.bid

    if ss.delay_entry_points_live > 0:
        if sinal_normal['type'] == 'BUY':
            preco_gatilho_com_delay = round(preco_sinal_original + ss.delay_entry_points_live * point, digits)
            if tick.ask > preco_gatilho_com_delay:
                 log_st_e_arquivo(f"Processar Sinal Normal (BUY): Preço ASK atual ({tick.ask:.{digits}f}) pior que gatilho com delay ({preco_gatilho_com_delay:.{digits}f}). Sinal ignorado.", "INFO")
                 ss.pending_signal_live = None; return
        else: # SELL
            preco_gatilho_com_delay = round(preco_sinal_original - ss.delay_entry_points_live * point, digits)
            if tick.bid < preco_gatilho_com_delay:
                 log_st_e_arquivo(f"Processar Sinal Normal (SELL): Preço BID atual ({tick.bid:.{digits}f}) pior que gatilho com delay ({preco_gatilho_com_delay:.{digits}f}). Sinal ignorado.", "INFO")
                 ss.pending_signal_live = None; return

    sl_abs, tp_abs = 0.0, 0.0
    if ss.stop_loss_pts_live > 0:
        sl_abs = round(execution_price - ss.stop_loss_pts_live * point, digits) if sinal_normal['type'] == 'BUY' else round(execution_price + ss.stop_loss_pts_live * point, digits)
    if ss.stop_win_pts_live > 0:
        tp_abs = round(execution_price + ss.stop_win_pts_live * point, digits) if sinal_normal['type'] == 'BUY' else round(execution_price - ss.stop_win_pts_live * point, digits)

    if ss.max_loss_trade_financeiro_live > 0 and ss.stop_loss_pts_live > 0 :
        valor_por_ponto_ativo_live = 0.0
        if s_info.trade_tick_value != 0 and s_info.trade_tick_size != 0 and point !=0 :
             valor_por_ponto_ativo_live = (s_info.trade_tick_value / s_info.trade_tick_size) / (point / s_info.trade_tick_size if s_info.trade_tick_size != 0 else 1)


        if valor_por_ponto_ativo_live > 0:
            perda_com_sl_original_em_reais = ss.stop_loss_pts_live * valor_por_ponto_ativo_live * vol_validado
            if perda_com_sl_original_em_reais > ss.max_loss_trade_financeiro_live:
                novos_pontos_sl_calc = (ss.max_loss_trade_financeiro_live / (valor_por_ponto_ativo_live * vol_validado)) if (valor_por_ponto_ativo_live * vol_validado) > 0 else 0
                if novos_pontos_sl_calc > 0 :
                    sl_abs = round(execution_price - novos_pontos_sl_calc * point, digits) if sinal_normal['type'] == 'BUY' else round(execution_price + novos_pontos_sl_calc * point, digits)
                    log_st_e_arquivo(f"SL Normal ajustado por stop financeiro. Original pts: {ss.stop_loss_pts_live}, Novos pts: {novos_pontos_sl_calc:.2f}. Novo SL price: {sl_abs:.{digits}f}", "INFO")
                else:
                    log_st_e_arquivo(f"SL Normal: Cálculo de ajuste financeiro resultou em 0 pontos. Mantendo SL original ou nenhum.", "WARN")


    request = {
        "action": mt5.TRADE_ACTION_DEAL, "symbol": ativo, "volume": vol_validado,
        "type": mt5.ORDER_TYPE_BUY if sinal_normal['type'] == 'BUY' else mt5.ORDER_TYPE_SELL,
        "price": execution_price,
        "sl": sl_abs if sl_abs != 0.0 else 0.0,
        "tp": tp_abs if tp_abs != 0.0 else 0.0,
        "magic": MT5_MAGIC_NUMBER, "comment": f"Normal_{sinal_normal['type']}_ST",
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_FOK
    }
    if s_info and hasattr(s_info, 'filling_modes') and s_info.filling_modes:
        if mt5.ORDER_FILLING_FOK in s_info.filling_modes: request["type_filling"] = mt5.ORDER_FILLING_FOK
        elif mt5.ORDER_FILLING_IOC in s_info.filling_modes: request["type_filling"] = mt5.ORDER_FILLING_IOC

    order_result = None
    try:
        order_result = mt5.order_send(request)
    except Exception as e_os:
        log_st_e_arquivo(f"Processar Sinal Normal: Exceção order_send: {e_os}", "CRITICAL")
        return

    if order_result is None or order_result.retcode != mt5.TRADE_RETCODE_DONE:
        err_cmt = order_result.comment if order_result else "N/A"
        err_code_ret = order_result.retcode if order_result else "N/A"
        last_err_send_code, last_err_send_msg = 0, "N/A"
        try:
            err_tuple_send = mt5.last_error()
            if err_tuple_send: last_err_send_code, last_err_send_msg = err_tuple_send
        except Exception as e_le_psn:
            log_st_e_arquivo(f"Exceção mt5.last_error() em processar_sinal_pendente: {e_le_psn}", "WARN")
        log_st_e_arquivo(f"Processar Sinal Normal: Falha ao enviar ordem. Com: {err_cmt}, Cod Ret: {err_code_ret}, Erro MT5 Cód: {last_err_send_code}, Msg: {last_err_send_msg}", "ERROR")
    else:
        log_st_e_arquivo(f"Processar Sinal Normal: Ordem {sinal_normal['type']} enviada. Ticket Posição: {order_result.position if order_result.position > 0 else order_result.order}. Preço Exec: {order_result.price}", "INFO")
        ss.pending_signal_live = None

    time.sleep(0.2)

# --- Funções de Lógica do Gradiente Averaging (Live) ---
def _st_resetar_ciclo_ga_live():
    ss = st.session_state
    log_st_e_arquivo("GA Live: Resetando informações do ciclo.", "INFO")
    ss.ga_live_ciclo_ativo = False
    ss.ga_live_ciclo_info = {}
    ss.ga_live_ciclo_posicoes_ids = []
    ss.ga_live_ciclo_nivel_atual = 0
    ss.ga_live_ciclo_proximo_nivel_preco_ativacao = 0.0
    ss.pending_signal_ga_live = None

def _st_iniciar_ciclo_ga_live(tipo_base, preco_base_sinal, symbol):
    ss = st.session_state
    if ss.ga_live_ciclo_ativo:
        log_st_e_arquivo("GA Live: Tentativa de iniciar novo ciclo GA, mas um já está ativo.", "WARN")
        return False

    log_st_e_arquivo(f"GA Live: Iniciando novo ciclo {tipo_base} para {symbol} @ Sinal {preco_base_sinal}, Vol Inicial: {ss.ga_live_vol_inicial}", "INFO")
    vol_validado = validar_volume_st(symbol, ss.ga_live_vol_inicial)
    if vol_validado == 0.0:
        log_st_e_arquivo(f"GA Live: Volume inicial {ss.ga_live_vol_inicial} inválido (resultou em {vol_validado}). Ciclo não iniciado.", "ERROR")
        return False

    s_info = None; tick_info = None
    try:
        s_info = mt5.symbol_info(symbol)
        tick_info = mt5.symbol_info_tick(symbol)
    except Exception as e:
        log_st_e_arquivo(f"GA Live Iniciar: Erro ao obter info/tick para {symbol}: {e}", "ERROR"); return False
    if not s_info or not tick_info:
        log_st_e_arquivo(f"GA Live Iniciar: Falha info/tick para {symbol}. Ciclo não iniciado.", "ERROR"); return False

    preco_execucao_base = tick_info.ask if tipo_base == 'BUY' else tick_info.bid
    request = {
        "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": vol_validado,
        "type": mt5.ORDER_TYPE_BUY if tipo_base == 'BUY' else mt5.ORDER_TYPE_SELL,
        "price": preco_execucao_base, "magic": MT5_MAGIC_NUMBER, "comment": f"GA_Base_{tipo_base}",
        "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_FOK
    }
    if s_info and hasattr(s_info, 'filling_modes') and s_info.filling_modes:
        if mt5.ORDER_FILLING_FOK in s_info.filling_modes: request["type_filling"] = mt5.ORDER_FILLING_FOK
        elif mt5.ORDER_FILLING_IOC in s_info.filling_modes: request["type_filling"] = mt5.ORDER_FILLING_IOC

    result = None
    try:
        result = mt5.order_send(request)
    except Exception as e_ord:
        log_st_e_arquivo(f"GA Live Iniciar: Exceção order_send: {e_ord}", "CRITICAL"); return False

    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        err_cmt = result.comment if result else "N/A"; err_code_ret = result.retcode if result else "N/A"
        last_err_send_code, last_err_send_msg = 0, "N/A"
        try:
            err_tuple_send = mt5.last_error()
            if err_tuple_send: last_err_send_code, last_err_send_msg = err_tuple_send
        except Exception as e_le_icga:
            log_st_e_arquivo(f"Exceção mt5.last_error() em iniciar ciclo GA: {e_le_icga}", "WARN")
        log_st_e_arquivo(f"GA Live: Falha ao enviar ordem base. Com: {err_cmt}, Cod Ret: {err_code_ret}, Erro MT5 Cód: {last_err_send_code}, Msg: {last_err_send_msg}", "ERROR")
        return False

    log_st_e_arquivo(f"GA Live: Ordem base {tipo_base} enviada. Ticket Posição: {result.position if result.position > 0 else result.order}. Preço Exec: {result.price}", "INFO")
    ss.ga_live_ciclo_ativo = True
    ss.ga_live_ciclo_info = {
        'tipo_ordem_base': tipo_base, 'symbol': symbol,
        'pnl_flutuante_total': 0.0, 'pnl_realizado_no_ciclo': 0.0,
        'volume_total_aberto': vol_validado, 'preco_medio_ponderado': result.price,
        'primeira_ordem_preco': result.price
    }
    if result.position > 0:
      ss.ga_live_ciclo_posicoes_ids = [result.position]
    else:
      ss.ga_live_ciclo_posicoes_ids = []
      log_st_e_arquivo(f"GA Live Iniciar: Ordem base não resultou em ID de posição > 0 (ID: {result.position}). Checar trades.", "WARN")


    ss.ga_live_ciclo_nivel_atual = 1
    point, digits = s_info.point, s_info.digits
    dist_grid_pts = ss.ga_live_dist_base_pts
    if tipo_base == 'BUY':
        ss.ga_live_ciclo_proximo_nivel_preco_ativacao = round(result.price - dist_grid_pts * point, digits)
    else: # SELL
        ss.ga_live_ciclo_proximo_nivel_preco_ativacao = round(result.price + dist_grid_pts * point, digits)
    log_st_e_arquivo(f"GA Live: Ciclo iniciado. Nv.1. Próx. Nv. {tipo_base} em {ss.ga_live_ciclo_proximo_nivel_preco_ativacao:.{digits}f}", "INFO")
    ss.pending_signal_ga_live = None
    return True

def _st_adicionar_nivel_ga_live(symbol):
    ss = st.session_state
    log_st_e_arquivo(f"GA Live: Adicionando novo nível ao ciclo para {symbol}.", "INFO")
    vol_nivel_sub = validar_volume_st(symbol, ss.ga_live_vol_nivel_sub)
    if vol_nivel_sub == 0.0:
        log_st_e_arquivo(f"GA Live: Volume de nível subsequente ({ss.ga_live_vol_nivel_sub}) inválido (resultou em {vol_nivel_sub}). Nível não adicionado.", "ERROR")
        return

    tipo_base = ss.ga_live_ciclo_info['tipo_ordem_base']
    s_info = None; tick_info = None
    try:
        s_info = mt5.symbol_info(symbol)
        tick_info = mt5.symbol_info_tick(symbol)
    except Exception as e:
        log_st_e_arquivo(f"GA Live Add Nivel: Erro info/tick {symbol}: {e}", "ERROR"); return
    if not s_info or not tick_info:
        log_st_e_arquivo(f"GA Live Add Nivel: Falha info/tick {symbol}. Nível não adicionado.", "ERROR"); return

    preco_exec_novo_nivel = tick_info.ask if tipo_base == 'BUY' else tick_info.bid
    request = {
        "action": mt5.TRADE_ACTION_DEAL, "symbol": symbol, "volume": vol_nivel_sub,
        "type": mt5.ORDER_TYPE_BUY if tipo_base == 'BUY' else mt5.ORDER_TYPE_SELL,
        "price": preco_exec_novo_nivel, "magic": MT5_MAGIC_NUMBER,
        "comment": f"GA_Nivel_{ss.ga_live_ciclo_nivel_atual + 1}",
        "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_FOK
    }
    if s_info and hasattr(s_info, 'filling_modes') and s_info.filling_modes:
        if mt5.ORDER_FILLING_FOK in s_info.filling_modes: request["type_filling"] = mt5.ORDER_FILLING_FOK
        elif mt5.ORDER_FILLING_IOC in s_info.filling_modes: request["type_filling"] = mt5.ORDER_FILLING_IOC

    result = None
    try:
        result = mt5.order_send(request)
    except Exception as e_ord_nv:
        log_st_e_arquivo(f"GA Live Add Nivel: Exceção order_send: {e_ord_nv}", "CRITICAL"); return

    if result is None or result.retcode != mt5.TRADE_RETCODE_DONE:
        err_cmt = result.comment if result else "N/A"; err_code_ret = result.retcode if result else "N/A"
        last_err_add_code, last_err_add_msg = 0, "N/A"
        try:
            err_tuple_add = mt5.last_error()
            if err_tuple_add: last_err_add_code, last_err_add_msg = err_tuple_add
        except Exception as e_le_anlga:
            log_st_e_arquivo(f"Exceção mt5.last_error() em adicionar nível GA: {e_le_anlga}", "WARN")
        log_st_e_arquivo(f"GA Live: Falha ao enviar ordem de novo nível. Com: {err_cmt}, Cod Ret: {err_code_ret}, MT5 Err Cód: {last_err_add_code}, Msg: {last_err_add_msg}", "ERROR")
        return

    log_st_e_arquivo(f"GA Live: Ordem de Nível {ss.ga_live_ciclo_nivel_atual + 1} enviada. Ticket Posição: {result.position if result.position > 0 else result.order}. Preço Exec: {result.price}", "INFO")

    if result.position > 0 and result.position not in ss.ga_live_ciclo_posicoes_ids:
         ss.ga_live_ciclo_posicoes_ids.append(result.position)
    elif result.position == 0:
         log_st_e_arquivo(f"GA Live Add Nivel: Ordem de nível não resultou em ID de posição > 0 (ID: {result.position}). Checar trades.", "WARN")


    ss.ga_live_ciclo_nivel_atual += 1
    _st_atualizar_info_ciclo_ga_live(symbol)

    point, digits = s_info.point, s_info.digits
    dist_grid_pts = ss.ga_live_dist_base_pts

    ref_price_for_next_level = result.price

    if tipo_base == 'BUY':
        ss.ga_live_ciclo_proximo_nivel_preco_ativacao = round(ref_price_for_next_level - dist_grid_pts * point, digits)
    else: # SELL
        ss.ga_live_ciclo_proximo_nivel_preco_ativacao = round(ref_price_for_next_level + dist_grid_pts * point, digits)
    log_st_e_arquivo(f"GA Live: Nível {ss.ga_live_ciclo_nivel_atual} adicionado. Próx. Nv. {tipo_base} em {ss.ga_live_ciclo_proximo_nivel_preco_ativacao:.{digits}f}", "INFO")

def _st_atualizar_info_ciclo_ga_live(symbol):
    ss = st.session_state
    if not ss.ga_live_ciclo_ativo or not ss.ga_live_ciclo_info.get('symbol') == symbol:
        return

    all_positions_robo = None
    try:
        all_positions_robo = mt5.positions_get(symbol=symbol, magic=MT5_MAGIC_NUMBER)
    except Exception as e_pos_upd:
        log_st_e_arquivo(f"GA Live Atualizar Info: Erro mt5.positions_get: {e_pos_upd}", "ERROR")
        return

    posicoes_do_ciclo_atual = []
    if all_positions_robo:
        tipo_base_ciclo = ss.ga_live_ciclo_info.get('tipo_ordem_base')
        if tipo_base_ciclo:
            expected_pos_type = mt5.ORDER_TYPE_BUY if tipo_base_ciclo == 'BUY' else mt5.ORDER_TYPE_SELL
            posicoes_do_ciclo_atual = [p for p in all_positions_robo if p.type == expected_pos_type]
        else:
            posicoes_do_ciclo_atual = list(all_positions_robo)


    if not posicoes_do_ciclo_atual:
        log_st_e_arquivo(f"GA Live: Nenhuma posição ativa encontrada para o ciclo de {symbol} (tipo {ss.ga_live_ciclo_info.get('tipo_ordem_base', 'N/A')}). Resetando ciclo.", "INFO")
        _st_resetar_ciclo_ga_live()
        return

    total_volume_ciclo = 0.0
    weighted_sum_price_ciclo = 0.0
    current_pnl_flutuante_ciclo = 0.0
    current_pos_ids_in_cycle_rebuilt = []

    for pos in posicoes_do_ciclo_atual:
        total_volume_ciclo += pos.volume
        weighted_sum_price_ciclo += pos.price_open * pos.volume
        current_pnl_flutuante_ciclo += pos.profit
        current_pos_ids_in_cycle_rebuilt.append(pos.ticket)

    preco_medio_pond_ciclo = weighted_sum_price_ciclo / total_volume_ciclo if total_volume_ciclo > 0 else 0.0

    ss.ga_live_ciclo_info['volume_total_aberto'] = total_volume_ciclo
    ss.ga_live_ciclo_info['preco_medio_ponderado'] = preco_medio_pond_ciclo
    ss.ga_live_ciclo_info['pnl_flutuante_total'] = current_pnl_flutuante_ciclo
    ss.ga_live_ciclo_posicoes_ids = current_pos_ids_in_cycle_rebuilt

def _st_fechar_posicoes_ciclo_ga_live(symbol, comment="GA Ciclo Fechado ST"):
    ss = st.session_state
    if not ss.ga_live_ciclo_ativo:
        log_st_e_arquivo("GA Live Fechar: Ciclo GA não está ativo para fechar.", "INFO")
        return

    log_st_e_arquivo(f"GA Live: Tentando fechar todas as posições do ciclo GA para {symbol}. Motivo: {comment}", "INFO")
    tipo_base_ciclo = ss.ga_live_ciclo_info.get('tipo_ordem_base')
    if not tipo_base_ciclo:
        log_st_e_arquivo("GA Live Fechar: Tipo base do ciclo não definido. Não é possível fechar.", "ERROR")
        _st_resetar_ciclo_ga_live()
        return

    posicoes_ativas_robo_neste_symbol = None
    try:
        posicoes_ativas_robo_neste_symbol = mt5.positions_get(symbol=symbol, magic=MT5_MAGIC_NUMBER)
    except Exception as e_fga:
        log_st_e_arquivo(f"GA Live Fechar: Erro ao obter posições: {e_fga}", "ERROR")
        return

    if not posicoes_ativas_robo_neste_symbol:
        log_st_e_arquivo(f"GA Live Fechar: Nenhuma posição ATIVA encontrada para {symbol} com magic {MT5_MAGIC_NUMBER} para fechar. Ciclo resetado.", "WARN")
        _st_resetar_ciclo_ga_live()
        return

    s_info_fill = None; tick_info_fill = None
    try:
        s_info_fill = mt5.symbol_info(symbol)
        tick_info_fill = mt5.symbol_info_tick(symbol)
    except Exception as e_fif:
        log_st_e_arquivo(f"GA Live Fechar: Erro info/tick {symbol}: {e_fif}", "ERROR"); return
    if not s_info_fill or not tick_info_fill:
        log_st_e_arquivo(f"GA Live Fechar: Falha info/tick {symbol}. Não é possível fechar.", "ERROR"); return

    pnl_total_realizado_neste_fechamento = 0.0
    posicoes_fechadas_com_sucesso_count = 0

    for pos_to_close in posicoes_ativas_robo_neste_symbol:
        expected_mt5_pos_type = mt5.ORDER_TYPE_BUY if tipo_base_ciclo == 'BUY' else mt5.ORDER_TYPE_SELL
        if pos_to_close.type != expected_mt5_pos_type:
            log_st_e_arquivo(f"GA Live Fechar: Posição {pos_to_close.ticket} (tipo {pos_to_close.type}) não corresponde ao tipo do ciclo GA ({tipo_base_ciclo}). Ignorando.", "WARN")
            continue

        close_order_type = mt5.ORDER_TYPE_SELL if pos_to_close.type == mt5.ORDER_TYPE_BUY else mt5.ORDER_TYPE_BUY
        price_close = tick_info_fill.bid if close_order_type == mt5.ORDER_TYPE_SELL else tick_info_fill.ask

        request_close = {
            "action": mt5.TRADE_ACTION_DEAL, "position": pos_to_close.ticket, "symbol": symbol,
            "volume": pos_to_close.volume, "type": close_order_type, "price": price_close,
            "deviation": 20, "magic": MT5_MAGIC_NUMBER, "comment": comment,
            "type_time": mt5.ORDER_TIME_GTC, "type_filling": mt5.ORDER_FILLING_FOK
        }
        if s_info_fill and hasattr(s_info_fill, 'filling_modes') and s_info_fill.filling_modes: 
             if mt5.ORDER_FILLING_FOK in s_info_fill.filling_modes: request_close["type_filling"] = mt5.ORDER_FILLING_FOK
             elif mt5.ORDER_FILLING_IOC in s_info_fill.filling_modes: request_close["type_filling"] = mt5.ORDER_FILLING_IOC

        result_close = None
        try:
            result_close = mt5.order_send(request_close)
        except Exception as e_ord_f:
            log_st_e_arquivo(f"GA Live Fechar: Exceção order_send para pos {pos_to_close.ticket}: {e_ord_f}", "CRITICAL"); continue 

        if result_close is None or result_close.retcode != mt5.TRADE_RETCODE_DONE:
            err_cmt_close = result_close.comment if result_close else "N/A"; err_code_ret_close = result_close.retcode if result_close else "N/A" 
            last_err_close_code, last_err_close_msg = 0, "N/A"
            try:
                err_tuple_close = mt5.last_error()
                if err_tuple_close: last_err_close_code, last_err_close_msg = err_tuple_close
            except Exception as e_le_fga:
                log_st_e_arquivo(f"Exceção mt5.last_error() em fechar GA pos: {e_le_fga}", "WARN")
            log_st_e_arquivo(f"GA Live Fechar: Falha ao fechar posição {pos_to_close.ticket}. Com: {err_cmt_close}, Cod Ret: {err_code_ret_close}, MT5 Err Cód: {last_err_close_code}, Msg: {last_err_close_msg}", "ERROR")
        else:
            log_st_e_arquivo(f"GA Live Fechar: Posição {pos_to_close.ticket} (Vol: {pos_to_close.volume}) fechada. Deal: {result_close.deal}.", "INFO")
            posicoes_fechadas_com_sucesso_count +=1
            time.sleep(0.2) 

            deals_fechamento = None
            if result_close.deal > 0: 
                try:
                    deals_fechamento = mt5.history_deals_get(ticket=result_close.deal)
                except Exception as e_hdg:
                    log_st_e_arquivo(f"GA Live Fechar: Exceção history_deals_get para deal {result_close.deal}: {e_hdg}", "WARN")

                if deals_fechamento and len(deals_fechamento) > 0:
                    pnl_da_posicao_fechada = deals_fechamento[0].profit
                    pnl_total_realizado_neste_fechamento += pnl_da_posicao_fechada
                    ss.processed_deal_tickets_live.add(deals_fechamento[0].ticket)
                else:
                    log_st_e_arquivo(f"GA Live Fechar: Não foi possível obter info do deal {result_close.deal} para PNL da posição {pos_to_close.ticket}. PNL do fechamento não contabilizado aqui.", "WARN")
            else:
                log_st_e_arquivo(f"GA Live Fechar: Deal ticket {result_close.deal} inválido para pos {pos_to_close.ticket}. PNL não contabilizado aqui.", "WARN")


    if pnl_total_realizado_neste_fechamento != 0.0:
         if 'pnl_realizado_no_ciclo' in ss.ga_live_ciclo_info: 
            ss.ga_live_ciclo_info['pnl_realizado_no_ciclo'] = ss.ga_live_ciclo_info.get('pnl_realizado_no_ciclo', 0.0) + pnl_total_realizado_neste_fechamento
         log_st_e_arquivo(f"GA Live: PNL total realizado no fechamento deste ciclo (localmente): {pnl_total_realizado_neste_fechamento:.2f}. PNL do ciclo atualizado.", "INFO")

    _st_resetar_ciclo_ga_live()


def processar_sinal_pendente_ga_live_inicio():
    ss = st.session_state
    if not ss.pending_signal_ga_live or IS_ON_STREAMLIT_CLOUD or not ss.get("mt5_esta_conectado", False):
        if ss.pending_signal_ga_live and (IS_ON_STREAMLIT_CLOUD or not ss.get("mt5_esta_conectado", False)): 
            log_st_e_arquivo("Processar Sinal Início GA: Ignorado (Cloud ou MT5 desconectado).", "DEBUG")
        ss.pending_signal_ga_live = None 
        return

    sinal_ga = ss.pending_signal_ga_live.copy() 
    log_st_e_arquivo(f"Processando Sinal Pendente Início GA: {sinal_ga['type']} em {ss.ativo_principal_live}", "INFO")

    if ss.stop_diario_atingido_hoje_live:
        log_st_e_arquivo("Início Ciclo GA Cancelado: Stop Diário Atingido.", "WARN")
        ss.pending_signal_ga_live = None
        return

    current_positions_check = None
    try:
        current_positions_check = mt5.positions_get(symbol=ss.ativo_principal_live, magic=MT5_MAGIC_NUMBER)
    except Exception as e_pc_ga:
        log_st_e_arquivo(f"Processar Sinal GA: Erro ao verificar posições existentes: {e_pc_ga}", "ERROR")
        return 

    if current_positions_check and len(current_positions_check) > 0:
        log_st_e_arquivo(f"Processar Sinal GA: Já existe(m) {len(current_positions_check)} posição(ões) para {ss.ativo_principal_live} (Magic: {MT5_MAGIC_NUMBER}). Sinal de início GA ignorado.", "INFO")
        ss.pending_signal_ga_live = None 
        return

    sucesso_inicio_ga = _st_iniciar_ciclo_ga_live(
        tipo_base=sinal_ga['type'],
        preco_base_sinal=sinal_ga['price_signal'], 
        symbol=ss.ativo_principal_live
    )
    if sucesso_inicio_ga:
        log_st_e_arquivo("Ciclo GA iniciado com sucesso a partir de sinal pendente.", "INFO")
        ss.pending_signal_ga_live = None 
    else:
        log_st_e_arquivo("Falha ao iniciar ciclo GA a partir de sinal pendente. Sinal mantido para possível nova tentativa.", "ERROR")
        
# --- Thread Principal do Robô Live ---
def funcao_alvo_thread_robo_live(stop_event_param):
    ss = st.session_state; thread_name = threading.current_thread().name
    log_st_e_arquivo(f"Thread Robô Live ({thread_name}) iniciada.", "INFO")

    if IS_ON_STREAMLIT_CLOUD:
        log_st_e_arquivo(f"Thread Robô Live ({thread_name}): Em Streamlit Cloud, operações live desabilitadas. Thread em modo idle.", "WARN")
        ss.status_interface = "Robô Live Desabilitado (Cloud)"
        while not stop_event_param.is_set(): stop_event_param.wait(timeout=5)
        log_st_e_arquivo(f"Thread Robô Live ({thread_name}) finalizando (Modo Cloud).", "INFO")
        ss.robo_esta_ligado = False; ss.robot_thread_deve_rodar = False
        return

    last_signal_check_time = time.time()
    SIGNAL_CHECK_INTERVAL = 2 

    while not stop_event_param.is_set():
        try:
            if not ss.get("robo_esta_ligado", False): 
                while not ss.get("robo_esta_ligado", False) and not stop_event_param.is_set():
                    stop_event_param.wait(timeout=0.5) 
                if stop_event_param.is_set(): break 

            if not ss.get("mt5_esta_conectado", False):
                log_st_e_arquivo(f"Thread Live: MT5 não conectado. Aguardando...", "WARN")
                ss.status_interface = "Robô Live: Aguardando MT5"
                stop_event_param.wait(timeout=ss.intervalo_robo_segundos); continue

            ss.status_interface = "Robô Live: Operando" 
            atualizar_pnl_diario_historico_st() 
            
            if ss.stop_diario_atingido_hoje_live:
                if ss.status_interface != "Robô Live: Stop Diário Atingido": 
                    log_st_e_arquivo("Thread Live: Stop Diário atingido. Novas entradas e gerenciamento GA de novas entradas bloqueados.", "WARN")
                    ss.status_interface = "Robô Live: Stop Diário Atingido"
                
            horario_ok = verificar_horario_operacao_st()
            if not horario_ok:
                if ss.status_interface != "Robô Live: Fora de Horário":
                     log_st_e_arquivo("Thread Live: Fora do horário de operação.", "INFO")
                     ss.status_interface = "Robô Live: Fora de Horário"
                
                if not (ss.ga_live_ativo_checkbox and ss.ga_live_ciclo_ativo):
                    stop_event_param.wait(timeout=ss.intervalo_robo_segundos); continue


            ativo_selecionado = ss.ativo_principal_live
            s_info_thread = None; tick_thread = None
            try:
                s_info_thread = mt5.symbol_info(ativo_selecionado)
                tick_thread = mt5.symbol_info_tick(ativo_selecionado)
            except Exception as e_stt:
                log_st_e_arquivo(f"Thread Live: Erro info/tick {ativo_selecionado}: {e_stt}", "ERROR")
                stop_event_param.wait(timeout=ss.intervalo_robo_segundos); continue 

            if not s_info_thread or not tick_thread:
                log_st_e_arquivo(f"Thread Live: Falha info/tick {ativo_selecionado}. Aguardando.", "WARN")
                stop_event_param.wait(timeout=ss.intervalo_robo_segundos); continue

            point_live, digits_live = s_info_thread.point, s_info_thread.digits
            preco_ask_atual, preco_bid_atual = tick_thread.ask, tick_thread.bid

            if ss.ga_live_ativo_checkbox: 
                if ss.ga_live_ciclo_ativo: 
                    _st_atualizar_info_ciclo_ga_live(ativo_selecionado) 
                    if not ss.ga_live_ciclo_ativo: 
                        stop_event_param.wait(timeout=1); continue

                    pnl_sw_ciclo_ga_pts_cfg = ss.ga_live_pnl_sw_ciclo_pts
                    if pnl_sw_ciclo_ga_pts_cfg > 0:
                        pm_ciclo = ss.ga_live_ciclo_info.get('preco_medio_ponderado', 0.0)
                        tipo_base_ga_ciclo = ss.ga_live_ciclo_info.get('tipo_ordem_base', '')
                        dist_atual_ciclo_pts = 0.0
                        if tipo_base_ga_ciclo == 'BUY' and pm_ciclo > 0: 
                            dist_atual_ciclo_pts = (preco_bid_atual - pm_ciclo) / point_live if point_live != 0 else 0
                        elif tipo_base_ga_ciclo == 'SELL' and pm_ciclo > 0:
                            dist_atual_ciclo_pts = (pm_ciclo - preco_ask_atual) / point_live if point_live != 0 else 0

                        if dist_atual_ciclo_pts >= pnl_sw_ciclo_ga_pts_cfg:
                            log_st_e_arquivo(f"GA Live: STOP WIN CICLO (Pontos) atingido! Dist: {dist_atual_ciclo_pts:.1f}pts (Meta: {pnl_sw_ciclo_ga_pts_cfg}pts). Fechando ciclo.", "INFO")
                            _st_fechar_posicoes_ciclo_ga_live(ativo_selecionado, "GA Ciclo SW Pts")
                            stop_event_param.wait(timeout=1); continue

                    max_loss_ciclo_ga_config = ss.max_loss_trade_financeiro_live 
                    if max_loss_ciclo_ga_config > 0:
                        pnl_flut_ciclo_atual = ss.ga_live_ciclo_info.get('pnl_flutuante_total', 0.0)
                        pnl_total_ciclo_para_sl = pnl_flut_ciclo_atual + ss.ga_live_ciclo_info.get('pnl_realizado_no_ciclo', 0.0)
                        if pnl_total_ciclo_para_sl <= -max_loss_ciclo_ga_config:
                            log_st_e_arquivo(f"GA Live: STOP LOSS FINANCEIRO CICLO atingido! PNL Total Ciclo: {pnl_total_ciclo_para_sl:.2f} (Limite: R${-max_loss_ciclo_ga_config:.2f}). Fechando ciclo.", "CRITICAL")
                            _st_fechar_posicoes_ciclo_ga_live(ativo_selecionado, "GA Ciclo SL Fin")
                            stop_event_param.wait(timeout=1); continue

                    if not ss.stop_diario_atingido_hoje_live and ss.ga_live_ciclo_nivel_atual < ss.ga_live_max_niveis:
                        preco_ativacao_prox_nivel = ss.ga_live_ciclo_proximo_nivel_preco_ativacao
                        tipo_base_ga_ciclo_nv = ss.ga_live_ciclo_info.get('tipo_ordem_base', '') 
                        condicao_add_nivel = False
                        if tipo_base_ga_ciclo_nv == 'BUY' and preco_ask_atual <= preco_ativacao_prox_nivel and preco_ativacao_prox_nivel > 0:
                            condicao_add_nivel = True
                        elif tipo_base_ga_ciclo_nv == 'SELL' and preco_bid_atual >= preco_ativacao_prox_nivel and preco_ativacao_prox_nivel > 0:
                            condicao_add_nivel = True

                        if condicao_add_nivel:
                            log_st_e_arquivo(f"GA Live: Preço de ativação ({preco_ativacao_prox_nivel:.{digits_live}f}) para novo nível GA atingido. Adicionando nível.", "INFO")
                            _st_adicionar_nivel_ga_live(ativo_selecionado)
                            
                else: 
                    if ss.stop_diario_atingido_hoje_live: 
                        if ss.pending_signal_ga_live: ss.pending_signal_ga_live = None 
                    elif ss.pending_signal_ga_live: 
                        processar_sinal_pendente_ga_live_inicio()
                    elif (time.time() - last_signal_check_time > SIGNAL_CHECK_INTERVAL) and horario_ok: 
                        last_signal_check_time = time.time()
                        posicoes_existentes = None
                        try: posicoes_existentes = mt5.positions_get(symbol=ativo_selecionado, magic=MT5_MAGIC_NUMBER)
                        except Exception as e_pne_ga: log_st_e_arquivo(f"GA Live (Pré-Sinal): Erro ao verificar posições: {e_pne_ga}", "WARN")

                        if not posicoes_existentes or len(posicoes_existentes) == 0:
                            df_dados_ga = obter_dados_mt5_st(ativo_selecionado, ss.timeframe_live, n_candles=250) 
                            if df_dados_ga is not None and not df_dados_ga.empty:
                                ichi_params_ga = {'tenkan_period': ss.tenkan_period_live,
                                                  'kijun_period': ss.kijun_period_live,
                                                  'senkou_b_period': ss.senkou_b_period_live}
                                df_ichi_ga = calcular_ichimoku_st(df_dados_ga.copy(), **ichi_params_ga) 
                                if not df_ichi_ga.empty and len(df_ichi_ga) >=2: 
                                    sinal_para_ga = gerar_sinal_ichimoku_st(df_ichi_ga, ichi_params_ga)
                                    if sinal_para_ga:
                                        log_st_e_arquivo(f"GA Live: Novo Sinal Ichimoku para INICIAR CICLO GA: {sinal_para_ga['type']} @ {sinal_para_ga['price_signal']}", "INFO")
                                        ss.pending_signal_ga_live = {**sinal_para_ga, 'timestamp_geracao': datetime.datetime.now()}
                                        processar_sinal_pendente_ga_live_inicio() 
                        else:
                            log_st_e_arquivo("GA Live (Pré-Sinal): Posições normais existentes. Não buscando sinal GA.", "DEBUG")


            else: 
                if ss.ga_live_ciclo_ativo: 
                    log_st_e_arquivo("GA Live: Checkbox GA desmarcado. Fechando ciclo GA ativo e resetando.", "INFO")
                    _st_fechar_posicoes_ciclo_ga_live(ativo_selecionado, "GA Desativado Manualmente UI")
                    stop_event_param.wait(timeout=1); continue 

                if ss.stop_diario_atingido_hoje_live: 
                    if ss.pending_signal_live: ss.pending_signal_live = None 
                elif ss.pending_signal_live: 
                    processar_sinal_pendente_st()
                elif (time.time() - last_signal_check_time > SIGNAL_CHECK_INTERVAL) and horario_ok: 
                    last_signal_check_time = time.time()
                    current_positions_normal = None
                    try:
                        current_positions_normal = mt5.positions_get(symbol=ativo_selecionado, magic=MT5_MAGIC_NUMBER)
                    except Exception as e_th_pos_n:
                        log_st_e_arquivo(f"Thread Live (Normal): Erro ao obter posições: {e_th_pos_n}", "ERROR")

                    if not current_positions_normal or len(current_positions_normal) == 0: 
                        df_dados_normal = obter_dados_mt5_st(ativo_selecionado, ss.timeframe_live, n_candles=250) 
                        if df_dados_normal is not None and not df_dados_normal.empty:
                            ichi_params_normal = {'tenkan_period': ss.tenkan_period_live,
                                                  'kijun_period': ss.kijun_period_live,
                                                  'senkou_b_period': ss.senkou_b_period_live}
                            df_ichimoku_normal = calcular_ichimoku_st(df_dados_normal.copy(), **ichi_params_normal)
                            if not df_ichimoku_normal.empty and len(df_ichimoku_normal) >=2:
                                sinal_ichimoku_gerado_normal = gerar_sinal_ichimoku_st(df_ichimoku_normal, ichi_params_normal)
                                if sinal_ichimoku_gerado_normal:
                                    log_st_e_arquivo(f"Thread Live (Normal): NOVO Sinal: {sinal_ichimoku_gerado_normal['type']} {ativo_selecionado} @ {sinal_ichimoku_gerado_normal['price_signal']}", "INFO")
                                    ss.pending_signal_live = {**sinal_ichimoku_gerado_normal, 'timestamp_geracao': datetime.datetime.now()}
                                    processar_sinal_pendente_st() 
                                    
        except Exception as e_loop_thread:
            log_st_e_arquivo(f"Erro CRÍTICO no loop principal do robô live: {e_loop_thread}", "CRITICAL")
            logger.exception("Detalhes da exceção no loop da thread live:") 
            ss.status_interface = "Robô Live: Erro Crítico Loop"
            stop_event_param.wait(timeout=ss.intervalo_robo_segundos * 5) 

        stop_event_param.wait(timeout=ss.intervalo_robo_segundos) 

    log_st_e_arquivo(f"Thread Robô Live ({thread_name}) finalizando...", "INFO")
    if ss.get("mt5_esta_conectado", False) and not IS_ON_STREAMLIT_CLOUD: 
        desligar_mt5_st() 
    ss.robot_thread_deve_rodar = False; ss.robo_esta_ligado = False
    ss.status_interface = "Robô Desligado (Thread Finalizada)"; ss.pending_signal_live = None
    _st_resetar_ciclo_ga_live() 


def acionar_ligar_robo_live():
    ss = st.session_state
    if IS_ON_STREAMLIT_CLOUD:
        st.warning("Operações Live estão desabilitadas no ambiente Streamlit Cloud."); return
    if not ss.get("robo_esta_ligado", False): 
        if not ss.get("mt5_esta_conectado", False):
            log_st_e_arquivo("Ligar Robô Live: MT5 não conectado. Tente conectar primeiro.", "WARN")
            st.warning("Conecte ao MetaTrader 5 antes de ligar o robô."); return

        ss.robo_esta_ligado = True; ss.robot_thread_deve_rodar = True 
        ss.status_interface = "Robô Live Ligando..."
        log_st_e_arquivo("Comando LIGAR ROBÔ LIVE recebido.", "INFO")

        if ss.data_ultimo_pnl_reset_live != datetime.date.today():
            atualizar_pnl_diario_historico_st() 

        if ss.stop_diario_atingido_hoje_live:
            log_st_e_arquivo("Ligar Robô Live: Robô ligado, mas Stop Diário já atingido para hoje.", "WARN")
            st.warning("Robô ligado, mas Stop Diário já atingido. Novas entradas bloqueadas.")

        _st_resetar_ciclo_ga_live() 
        ss.pending_signal_live = None 
        ss.pending_signal_ga_live = None 

        thread_atual = ss.get("robot_thread_referencia")
        if thread_atual is None or not thread_atual.is_alive():
            log_st_e_arquivo("Iniciando nova thread Robô Live...", "INFO")
            ss.stop_robot_event_obj.clear() 
            nova_thread_robo = threading.Thread(target=funcao_alvo_thread_robo_live,
                                                args=(ss.stop_robot_event_obj,),
                                                name="RoboWorkerLive", daemon=True)
            ss.robot_thread_referencia = nova_thread_robo
            nova_thread_robo.start()
        else:
            log_st_e_arquivo("Thread Robô Live já ativa. Robô apenas marcado como 'ligado'.", "INFO")
    else:
        st.info("O Robô Live já está ligado.")

def acionar_desligar_robo_live():
    ss = st.session_state
    ss.status_interface = "Robô Live Desligando...";
    log_st_e_arquivo("Comando DESLIGAR ROBÔ LIVE recebido.", "INFO")

    ss.robo_esta_ligado = False 
    ss.robot_thread_deve_rodar = False 

    if ss.ga_live_ativo_checkbox and ss.ga_live_ciclo_ativo and \
       ss.get("mt5_esta_conectado", False) and not IS_ON_STREAMLIT_CLOUD:
        log_st_e_arquivo("Desligando Robô: Fechando ciclo GA ativo.", "INFO")
        _st_fechar_posicoes_ciclo_ga_live(ss.ativo_principal_live, "Desligamento Manual Robô")
    
    ss.pending_signal_live = None 
    ss.pending_signal_ga_live = None

    thread_atual = ss.get("robot_thread_referencia")
    if thread_atual and thread_atual.is_alive():
        log_st_e_arquivo("Sinalizando para thread Robô Live parar (stop_event_obj.set())...", "INFO")
        ss.stop_robot_event_obj.set() 
    else:
        log_st_e_arquivo("Thread Robô Live não ativa ou já finalizada.", "DEBUG")
        if ss.get("mt5_esta_conectado", False) and not IS_ON_STREAMLIT_CLOUD:
            log_st_e_arquivo("Thread não ativa, mas MT5 conectado. Tentando desligar MT5.", "INFO")
            desligar_mt5_st() 
            
# --- Funções de Backtest ---
def obter_dados_historicos_st(symbol, timeframe_minutes, start_datetime, end_datetime):
    ss = st.session_state
    log_st_e_arquivo(f"BT: Buscando dados {symbol} de {start_datetime} a {end_datetime} TF{timeframe_minutes}.", "INFO")
    if IS_ON_STREAMLIT_CLOUD:
        log_st_e_arquivo("BT: Em Streamlit Cloud, dados históricos reais do MT5 não podem ser buscados.", "WARN")
        ss.bt_status_message = "BT: Não funcional em Cloud (sem dados MT5)."
        return pd.DataFrame(columns=['time', 'open', 'high', 'low', 'close', 'tick_volume', 'spread', 'real_volume'])

    mt5_orig_conn = ss.get("mt5_esta_conectado", False); temp_conn_used = False
    if not mt5_orig_conn:
        log_st_e_arquivo("BT: MT5 não conectado. Tentando conexão temporária para dados históricos.", "INFO")
        if conectar_mt5_st(ss.mt5_login_input, ss.mt5_password_input, ss.mt5_server_input):
            temp_conn_used = True
        else:
            log_st_e_arquivo("BT: Falha conexão MT5 temporária para obter dados.", "ERROR")
            ss.bt_status_message = "BT: Erro conexão MT5 p/ dados."; return None

    current_terminal_info_bt = None
    try:
        current_terminal_info_bt = mt5.terminal_info()
    except Exception as e_cti_bt:
        log_st_e_arquivo(f"BT: Exceção terminal_info: {e_cti_bt}. Não foi possível obter dados.", "ERROR")
        if temp_conn_used and ss.get("mt5_esta_conectado", False): desligar_mt5_st()
        ss.bt_status_message = "BT: Erro terminal MT5."; return None

    if current_terminal_info_bt is None: 
        log_st_e_arquivo("BT: Terminal MT5 não está inicializado para buscar dados.", "ERROR")
        if temp_conn_used and ss.get("mt5_esta_conectado", False): desligar_mt5_st()
        ss.bt_status_message = "BT: Erro terminal MT5 não inicializado."; return None

    tf_map = {1:mt5.TIMEFRAME_M1, 5:mt5.TIMEFRAME_M5, 15:mt5.TIMEFRAME_M15, 30:mt5.TIMEFRAME_M30, 60:mt5.TIMEFRAME_H1, 240:mt5.TIMEFRAME_H4, 1440:mt5.TIMEFRAME_D1}
    mt5_tf = tf_map.get(timeframe_minutes)
    if mt5_tf is None:
        log_st_e_arquivo(f"BT: Timeframe {timeframe_minutes}min inválido.", "ERROR")
        if temp_conn_used and ss.get("mt5_esta_conectado", False): desligar_mt5_st()
        return None

    s_info_hist = None
    try:
        s_info_hist = mt5.symbol_info(symbol)
    except Exception as e_sih:
        log_st_e_arquivo(f"BT: Exceção ao obter symbol_info para {symbol}: {e_sih}", "ERROR")
        if temp_conn_used and ss.get("mt5_esta_conectado", False): desligar_mt5_st()
        return None

    if s_info_hist is None:
        log_st_e_arquivo(f"BT: Símbolo {symbol} não encontrado no MT5.", "ERROR")
        if temp_conn_used and ss.get("mt5_esta_conectado", False): desligar_mt5_st()
        return None

    if not s_info_hist.visible:
        log_st_e_arquivo(f"BT: Símbolo {symbol} não visível. Tentando selecionar...", "INFO")
        selected_hist = False
        try:
            selected_hist = mt5.symbol_select(symbol, True)
        except Exception as e_ssh:
            log_st_e_arquivo(f"BT: Exceção symbol_select para {symbol}: {e_ssh}", "ERROR")

        if not selected_hist:
            last_err_sel_code, last_err_sel_msg = 0, "N/A"
            try:
                err_tuple_sel = mt5.last_error()
                if err_tuple_sel: last_err_sel_code, last_err_sel_msg = err_tuple_sel
            except Exception as e_le_sh:
                log_st_e_arquivo(f"Exceção mt5.last_error() em symbol_select (hist): {e_le_sh}", "WARN")
            log_st_e_arquivo(f"BT: Falha tornar {symbol} visível. Erro MT5 Cód: {last_err_sel_code}, Msg: {last_err_sel_msg}", "ERROR")
            if temp_conn_used and ss.get("mt5_esta_conectado", False): desligar_mt5_st()
            return None
        time.sleep(0.5) 
        try: s_info_hist = mt5.symbol_info(symbol)
        except: s_info_hist = None 
        if s_info_hist is None or not s_info_hist.visible:
            log_st_e_arquivo(f"BT: Mesmo após tentativa, {symbol} não visível ou info falhou.", "ERROR")
            if temp_conn_used and ss.get("mt5_esta_conectado", False): desligar_mt5_st()
            return None

    rates = None
    try:
        rates = mt5.copy_rates_range(symbol, mt5_tf, start_datetime, end_datetime)
    except Exception as e_cr_hist:
        log_st_e_arquivo(f"BT: Exceção ao chamar copy_rates_range para {symbol}: {e_cr_hist}", "ERROR")
        rates = None 

    if temp_conn_used and ss.get("mt5_esta_conectado", False): 
        log_st_e_arquivo("BT: Desligando conexão MT5 temporária após busca de dados.", "INFO")
        desligar_mt5_st()

    if rates is None or len(rates) == 0:
        last_err_code_hist_rates, last_err_msg_hist_rates = 0, "N/A"
        
        if ss.get("mt5_esta_conectado", False) and not IS_ON_STREAMLIT_CLOUD : 
            try:
                error_tuple_hist_rates = mt5.last_error()
                if error_tuple_hist_rates:
                    last_err_code_hist_rates = error_tuple_hist_rates[0]
                    last_err_msg_hist_rates = error_tuple_hist_rates[1]
            except Exception as e_le_hist_rates:
                log_st_e_arquivo(f"Exceção ao chamar mt5.last_error() em copy_rates_range: {e_le_hist_rates}", "WARN")

        log_st_e_arquivo(f"BT: Nenhum dado para {symbol} no período. Erro MT5 Cód: {last_err_code_hist_rates}, Msg: {last_err_msg_hist_rates}", "WARN")
        return None 

    df = pd.DataFrame(rates); df['time'] = pd.to_datetime(df['time'], unit='s'); df.set_index('time', inplace=True); df.sort_index(inplace=True)
    log_st_e_arquivo(f"BT: Dados históricos para {symbol} obtidos: {len(df)} barras.", "INFO")
    return df


def get_symbol_params_for_backtest_st(symbol_str):
    ss = st.session_state
    p_default, d_default, vp_default = 0.01, 2, 1.0 

    if "WIN" in symbol_str.upper(): p_default, d_default, vp_default = 1.0, 0, 0.20 
    elif "WDO" in symbol_str.upper(): p_default, d_default, vp_default = 0.5, 1, 10.00 
    elif "IND" in symbol_str.upper(): p_default, d_default, vp_default = 1.0, 0, 1.00 
    elif "DOL" in symbol_str.upper(): p_default, d_default, vp_default = 0.5, 1, 50.00 
    elif "JPY" in symbol_str.upper().replace("/", ""): p_default, d_default, vp_default = 0.001, 3, vp_default 
    elif any(currency in symbol_str.upper() for currency in ["EUR", "GBP", "AUD", "NZD", "CAD", "CHF"]):
        if len(symbol_str) == 6 : 
             p_default, d_default = 0.00001, 5 
        elif len(symbol_str) > 6 and "." in symbol_str : 
             p_default, d_default = 0.0001, 4 

    if IS_ON_STREAMLIT_CLOUD:
        log_st_e_arquivo(f"BT Params (Cloud) for {symbol_str}: Usando defaults P={p_default}, D={d_default}, VP={vp_default}.", "DEBUG")
        return p_default, d_default, vp_default

    mt5_orig_conn = ss.get("mt5_esta_conectado", False); temp_conn_used = False
    if not mt5_orig_conn:
        log_st_e_arquivo("BT Params: MT5 não conectado. Tentando conexão temporária.", "INFO")
        if conectar_mt5_st(ss.mt5_login_input, ss.mt5_password_input, ss.mt5_server_input):
            temp_conn_used = True
        else: 
            log_st_e_arquivo(f"BT Params: Falha conexão MT5 temp. Usando defaults para {symbol_str}: P={p_default}, D={d_default}, VP={vp_default}", "WARN")
            return p_default, d_default, vp_default

    p, d, vp = p_default, d_default, vp_default 

    if ss.get("mt5_esta_conectado", False): 
        term_info_params = None
        try:
            term_info_params = mt5.terminal_info()
        except Exception as e_tip:
            log_st_e_arquivo(f"BT Params: Exceção terminal_info: {e_tip}. Usando defaults.", "ERROR")

        if term_info_params: 
            s_info_params = None
            try:
                s_info_params = mt5.symbol_info(symbol_str)
            except Exception as e_sip:
                log_st_e_arquivo(f"BT Params: Exceção symbol_info para {symbol_str}: {e_sip}. Usando defaults.", "ERROR")

            if s_info_params:
                p, d = s_info_params.point, s_info_params.digits
                
                if s_info_params.trade_tick_value != 0 and s_info_params.trade_tick_size != 0 and s_info_params.point !=0:
                    ticks_per_point = s_info_params.point / s_info_params.trade_tick_size if s_info_params.trade_tick_size != 0 else 1
                    vp = (s_info_params.trade_tick_value / s_info_params.trade_tick_size) * (s_info_params.point / s_info_params.trade_tick_size) if s_info_params.trade_tick_size != 0 else vp_default # Corrected VP
                else: 
                    log_st_e_arquivo(f"BT Params: Não foi possível calcular VP para {symbol_str} com dados MT5. Usando default {vp_default}.", "WARN")
                    vp = vp_default 
            else: 
                log_st_e_arquivo(f"BT Params: Symbol info for {symbol_str} é None. Usando defaults P,D,VP.", "WARN")
        else: 
            log_st_e_arquivo(f"BT Params: terminal_info é None. Usando defaults P,D,VP.", "WARN")
    else: 
        log_st_e_arquivo(f"BT Params: Sem conexão MT5. Usando defaults para {symbol_str} P,D,VP.", "WARN")

    if temp_conn_used and ss.get("mt5_esta_conectado", False): 
        log_st_e_arquivo("BT Params: Desligando conexão MT5 temporária.", "INFO")
        desligar_mt5_st()

    log_st_e_arquivo(f"BT Params for {symbol_str}: Point={p}, Digits={d}, ValorPorPonto={vp}", "DEBUG")
    return p, d, vp

def _bt_gerar_sinal_ichimoku_st(df_hist, ichimoku_params_bt, symbol_for_point_info_unused=None):
    sinal = gerar_sinal_ichimoku_st(df_hist, ichimoku_params_bt)
    if sinal:
        sinal['trigger_price_ref_col'] = 'close'
    return sinal


def _bt_gerenciar_trade_normal_sl_tp_st(trade, bar_atual, valor_por_ponto, trades_realizados_lista, curva_equity_lista, balanco_corrente, timestamp_saida):
    if not trade: return None, None

    pnl_trade = None; razao_saida = None; preco_saida = 0.0

    if trade['type'] == 'BUY':
        if trade.get('sl_price') is not None and bar_atual['low'] <= trade['sl_price']:
            preco_saida = trade['sl_price']
            razao_saida = 'SL'
        elif trade.get('tp_price') is not None and bar_atual['high'] >= trade['tp_price']:
            preco_saida = trade['tp_price']
            razao_saida = 'TP'
    else: # SELL
        if trade.get('sl_price') is not None and bar_atual['high'] >= trade['sl_price']:
            preco_saida = trade['sl_price']
            razao_saida = 'SL'
        elif trade.get('tp_price') is not None and bar_atual['low'] <= trade['tp_price']:
            preco_saida = trade['tp_price']
            razao_saida = 'TP'

    if razao_saida:
        if trade['type'] == 'BUY':
            pnl_trade = (preco_saida - trade['entry_price']) * trade['volume'] * valor_por_ponto
        else: # SELL
            pnl_trade = (trade['entry_price'] - preco_saida) * trade['volume'] * valor_por_ponto

        trade_finalizado = {**trade,
                            'exit_price': preco_saida,
                            'exit_time': timestamp_saida,
                            'pnl': pnl_trade,
                            'reason': razao_saida}
        trades_realizados_lista.append(trade_finalizado)
        curva_equity_lista.append({'time': timestamp_saida, 'balance': balanco_corrente + pnl_trade})
        return None, pnl_trade

    return trade, None


def verificar_horario_operacao_bt_st(hora_inicio_str, hora_fim_str, current_datetime_obj):
    if not hora_inicio_str or not hora_fim_str: return True
    try:
        h_ini = datetime.datetime.strptime(hora_inicio_str, "%H:%M").time()
        h_fim = datetime.datetime.strptime(hora_fim_str, "%H:%M").time()
    except ValueError:
        log_st_e_arquivo(f"BT: Horário inválido: {hora_inicio_str} ou {hora_fim_str}. Permitindo operações.", "ERROR")
        return True

    agora_time_bt = current_datetime_obj.time()
    if h_ini <= h_fim:
        return h_ini <= agora_time_bt <= h_fim
    else:
        return agora_time_bt >= h_ini or agora_time_bt <= h_fim

def _bt_resetar_ciclo_ga_st():
    ss = st.session_state
    ss.bt_ga_ciclo_ativo = False
    ss.bt_ga_ciclo_info = {}

def _bt_iniciar_ciclo_ga_st(sinal_tipo, preco_entrada_candle, timestamp_entrada, vol_inicial_ga_bt, ponto_ativo_bt, digitos_ativo_bt):
    ss = st.session_state
    ss.bt_ga_ciclo_ativo = True
    ss.bt_ga_ciclo_info = {
        'tipo_base': sinal_tipo,
        'entry_time': timestamp_entrada,
        'nivel_atual': 1,
        'preco_medio_pond': preco_entrada_candle,
        'vol_total': vol_inicial_ga_bt,
        'ordens': [{'price': preco_entrada_candle, 'vol': vol_inicial_ga_bt, 'time': timestamp_entrada}],
        'primeira_ordem_preco': preco_entrada_candle,
        'pnl_realizado_no_ciclo': 0.0
    }
    log_st_e_arquivo(f"BT GA: Ciclo {sinal_tipo} iniciado @ {preco_entrada_candle:.{digitos_ativo_bt}f}, Vol: {vol_inicial_ga_bt:.2f}, Time: {timestamp_entrada}", "DEBUG")

def _bt_adicionar_nivel_ga_st(bar_atual_para_entrada, ponto_ativo_bt, digitos_ativo_bt, timestamp_adicao_nivel):
    ss = st.session_state
    if not ss.bt_ga_ciclo_ativo or not ss.bt_ga_ciclo_info.get('ordens'):
        log_st_e_arquivo("BT GA Add Nível: Ciclo não ativo ou sem ordens base.", "WARN")
        return

    if ss.bt_ga_ciclo_info['nivel_atual'] >= ss.bt_ga_max_niveis:
        return

    tipo_base_ciclo = ss.bt_ga_ciclo_info['tipo_base']
    preco_ultima_ordem_ciclo = ss.bt_ga_ciclo_info['ordens'][-1]['price']

    dist_niveis_cfg_pts = ss.bt_ga_dist_base_pts
    vol_subsequente_cfg = ss.bt_ga_vol_nivel_sub
    preco_ativacao_proximo_nivel_calc = 0.0
    preco_entrada_novo_nivel_simulado = 0.0

    if tipo_base_ciclo == 'BUY':
        preco_ativacao_proximo_nivel_calc = round(preco_ultima_ordem_ciclo - dist_niveis_cfg_pts * ponto_ativo_bt, digitos_ativo_bt)
        if bar_atual_para_entrada['low'] > preco_ativacao_proximo_nivel_calc : return
        preco_entrada_novo_nivel_simulado = min(bar_atual_para_entrada['open'], preco_ativacao_proximo_nivel_calc)

    else: # SELL
        preco_ativacao_proximo_nivel_calc = round(preco_ultima_ordem_ciclo + dist_niveis_cfg_pts * ponto_ativo_bt, digitos_ativo_bt)
        if bar_atual_para_entrada['high'] < preco_ativacao_proximo_nivel_calc : return
        preco_entrada_novo_nivel_simulado = max(bar_atual_para_entrada['open'], preco_ativacao_proximo_nivel_calc)

    log_st_e_arquivo(f"BT GA: Adicionando Nível {ss.bt_ga_ciclo_info['nivel_atual'] + 1} ao ciclo {tipo_base_ciclo} @ {preco_entrada_novo_nivel_simulado:.{digitos_ativo_bt}f}, Time: {timestamp_adicao_nivel}", "DEBUG")

    ss.bt_ga_ciclo_info['nivel_atual'] += 1
    ss.bt_ga_ciclo_info['ordens'].append({'price': preco_entrada_novo_nivel_simulado, 'vol': vol_subsequente_cfg, 'time': timestamp_adicao_nivel})

    novo_vol_total = sum(ordem['vol'] for ordem in ss.bt_ga_ciclo_info['ordens'])
    nova_soma_ponderada = sum(ordem['price'] * ordem['vol'] for ordem in ss.bt_ga_ciclo_info['ordens'])

    ss.bt_ga_ciclo_info['vol_total'] = novo_vol_total
    ss.bt_ga_ciclo_info['preco_medio_pond'] = round(nova_soma_ponderada / novo_vol_total, digitos_ativo_bt) if novo_vol_total > 0 else 0.0


def _bt_calcular_pnl_fechamento_ciclo_ga_st(preco_saida_ciclo, valor_por_ponto_ativo_bt):
    ss = st.session_state
    if not ss.bt_ga_ciclo_ativo or not ss.bt_ga_ciclo_info.get('ordens'):
        return 0.0

    total_pnl_ciclo_calculado = 0.0
    tipo_base_ciclo = ss.bt_ga_ciclo_info['tipo_base']

    for ordem in ss.bt_ga_ciclo_info['ordens']:
        if tipo_base_ciclo == 'BUY':
            pnl_ordem = (preco_saida_ciclo - ordem['price']) * ordem['vol'] * valor_por_ponto_ativo_bt
        else: # SELL
            pnl_ordem = (ordem['price'] - preco_saida_ciclo) * ordem['vol'] * valor_por_ponto_ativo_bt
        total_pnl_ciclo_calculado += pnl_ordem

    return total_pnl_ciclo_calculado


def _bt_gerenciar_ciclo_ga_sw_sl_st(bar_atual_bt, valor_por_ponto_ativo_bt, ponto_ativo_bt, digitos_ativo_bt, trades_realizados_lista_bt, curva_equity_lista_bt, balanco_corrente_bt, timestamp_atual_bt):
    ss = st.session_state
    if not ss.bt_ga_ciclo_ativo or not ss.bt_ga_ciclo_info.get('ordens'):
        return None

    pnl_do_ciclo_se_fechado = None
    preco_saida_simulada_ciclo = 0.0
    razao_saida_ciclo = ""

    pm_ciclo_bt = ss.bt_ga_ciclo_info['preco_medio_pond']
    tipo_base_ciclo_bt = ss.bt_ga_ciclo_info['tipo_base']

    sw_ciclo_ga_pts_cfg = ss.bt_ga_pnl_sw_ciclo_pts
    if sw_ciclo_ga_pts_cfg > 0:
        target_sw_price_ciclo_calc = 0.0
        sw_atingido_flag = False
        if tipo_base_ciclo_bt == 'BUY':
            target_sw_price_ciclo_calc = round(pm_ciclo_bt + sw_ciclo_ga_pts_cfg * ponto_ativo_bt, digitos_ativo_bt)
            if bar_atual_bt['high'] >= target_sw_price_ciclo_calc:
                sw_atingido_flag = True; preco_saida_simulada_ciclo = target_sw_price_ciclo_calc
        else: # SELL
            target_sw_price_ciclo_calc = round(pm_ciclo_bt - sw_ciclo_ga_pts_cfg * ponto_ativo_bt, digitos_ativo_bt)
            if bar_atual_bt['low'] <= target_sw_price_ciclo_calc:
                sw_atingido_flag = True; preco_saida_simulada_ciclo = target_sw_price_ciclo_calc

        if sw_atingido_flag:
            razao_saida_ciclo = "GA Ciclo SW Pts"
            pnl_do_ciclo_se_fechado = _bt_calcular_pnl_fechamento_ciclo_ga_st(preco_saida_simulada_ciclo, valor_por_ponto_ativo_bt)
            log_st_e_arquivo(f"BT GA: Ciclo {tipo_base_ciclo_bt} SW por PONTOS ATINGIDO. Preço Saída: {preco_saida_simulada_ciclo:.{digitos_ativo_bt}f}, PNL Ciclo: {pnl_do_ciclo_se_fechado:.2f}", "DEBUG")

    stop_financeiro_ciclo_cfg = ss.bt_stop_financeiro_trade
    if stop_financeiro_ciclo_cfg > 0 and pnl_do_ciclo_se_fechado is None:
        preco_para_calculo_pnl_flut_sl = 0.0
        if tipo_base_ciclo_bt == 'BUY':
            preco_para_calculo_pnl_flut_sl = bar_atual_bt['low']
        else: # SELL
            preco_para_calculo_pnl_flut_sl = bar_atual_bt['high']

        pnl_flutuante_estimado_ciclo = _bt_calcular_pnl_fechamento_ciclo_ga_st(preco_para_calculo_pnl_flut_sl, valor_por_ponto_ativo_bt)
        pnl_total_ciclo_para_sl = pnl_flutuante_estimado_ciclo + ss.bt_ga_ciclo_info.get('pnl_realizado_no_ciclo', 0.0)

        if pnl_total_ciclo_para_sl <= -stop_financeiro_ciclo_cfg:
            razao_saida_ciclo = "GA Ciclo SL Fin"
            preco_saida_simulada_ciclo = preco_para_calculo_pnl_flut_sl
            pnl_do_ciclo_se_fechado = _bt_calcular_pnl_fechamento_ciclo_ga_st(preco_saida_simulada_ciclo, valor_por_ponto_ativo_bt) 
            log_st_e_arquivo(f"BT GA: Ciclo {tipo_base_ciclo_bt} SL FINANCEIRO ATINGIDO. PNL Flut. Estimado no Pior Caso da Barra: {pnl_total_ciclo_para_sl:.2f} (Limite: {-stop_financeiro_ciclo_cfg:.2f}). Saída @ {preco_saida_simulada_ciclo:.{digitos_ativo_bt}f}, PNL Final Ciclo: {pnl_do_ciclo_se_fechado:.2f}", "DEBUG")

    if pnl_do_ciclo_se_fechado is not None:
        trade_agregado_ciclo_ga = {
            'type': f"GA_{ss.bt_ga_ciclo_info['tipo_base']}",
            'entry_price': ss.bt_ga_ciclo_info['preco_medio_pond'],
            'volume': ss.bt_ga_ciclo_info['vol_total'],
            'exit_price': preco_saida_simulada_ciclo,
            'entry_time': ss.bt_ga_ciclo_info.get('entry_time', timestamp_atual_bt),
            'exit_time': timestamp_atual_bt,
            'pnl': pnl_do_ciclo_se_fechado,
            'reason': razao_saida_ciclo
        }
        trades_realizados_lista_bt.append(trade_agregado_ciclo_ga)
        curva_equity_lista_bt.append({'time': timestamp_atual_bt, 'balance': balanco_corrente_bt + pnl_do_ciclo_se_fechado})
        _bt_resetar_ciclo_ga_st()
        return pnl_do_ciclo_se_fechado

    return None


def funcao_alvo_thread_backtest():
    ss = st.session_state
    log_st_e_arquivo("Thread de Backtest Iniciada.", "INFO")
    ss.bt_status_message = "BT: Em andamento..."; ss.bt_progresso = 0.0
    try:
        symbol = ss.bt_ativo; tf_mins = ss.bt_timeframe
        start_dt_obj = datetime.datetime.combine(ss.bt_start_date, datetime.time.min)
        end_dt_obj = datetime.datetime.combine(ss.bt_end_date, datetime.time.max)

        initial_balance_bt = ss.bt_initial_balance
        volume_base_bt_normal = ss.bt_volume
        sl_config_pts_normal, tp_config_pts_normal = ss.bt_sl_pts, ss.bt_tp_pts
        delay_entrada_pts_bt = ss.bt_delay_entry_pts 

        ichimoku_params_bt_cfg = {'tenkan_period': ss.bt_tenkan_period,
                                  'kijun_period': ss.bt_kijun_period,
                                  'senkou_b_period': ss.bt_senkou_b_period}
        hora_inicio_bt, hora_fim_bt = ss.bt_hora_inicio_op, ss.bt_hora_fim_op
        stop_financeiro_por_trade_cfg_bt = ss.bt_stop_financeiro_trade 
        stop_diario_total_financeiro_cfg_bt = ss.bt_stop_diario_total 
        fechar_no_stop_diario_cfg_bt = ss.bt_fechar_pos_stop_diario 

        kijun_bt = ichimoku_params_bt_cfg.get('kijun_period', 26)
        senkou_b_bt = ichimoku_params_bt_cfg.get('senkou_b_period', 52)
        buffer_ichimoku_candles = max(kijun_bt, senkou_b_bt) + kijun_bt + 5 

        candles_per_day_approx = (24*60) / tf_mins if tf_mins > 0 else 288 
        buffer_necessario_em_dias = int(buffer_ichimoku_candles / candles_per_day_approx) + 30 

        data_inicio_com_buffer = start_dt_obj - datetime.timedelta(days=buffer_necessario_em_dias)

        df_historico_completo = obter_dados_historicos_st(symbol, tf_mins, data_inicio_com_buffer, end_dt_obj)

        if df_historico_completo is None or df_historico_completo.empty:
            log_st_e_arquivo("BT: Falha ao obter dados históricos ou dados vazios. Backtest não pode continuar.", "ERROR")
            ss.bt_status_message = "Erro BT: Falha ao obter dados."
            ss.bt_esta_rodando = False; ss.bt_progresso = 100.0; return

        df_ichimoku_calculado_full = calcular_ichimoku_st(df_historico_completo.copy(), **ichimoku_params_bt_cfg)

        df_operacional = df_ichimoku_calculado_full[
            (df_ichimoku_calculado_full.index >= pd.to_datetime(start_dt_obj)) &
            (df_ichimoku_calculado_full.index <= pd.to_datetime(end_dt_obj))
        ].copy()

        if df_operacional.empty:
            log_st_e_arquivo("BT: DataFrame operacional vazio após filtro de data. Verifique o período e os dados.", "ERROR")
            ss.bt_status_message = "Erro BT: Dados insuficientes no período selecionado.";
            ss.bt_esta_rodando = False; ss.bt_progresso = 100.0; return

        log_st_e_arquivo(f"BT: {len(df_operacional)} barras para operar no período de {start_dt_obj.date()} a {end_dt_obj.date()}.", "INFO")

        ponto_ativo, digitos_ativo, valor_por_ponto_ativo = get_symbol_params_for_backtest_st(symbol)

        balanco_atual = initial_balance_bt
        pnl_do_dia_atual = 0.0
        data_iteracao_atual = None 
        stop_diario_atingido_flag = False

        lista_trades_executados = []
        lista_curva_equity = [{'time': df_operacional.index[0], 'balance': initial_balance_bt}]

        trade_ativo_no_momento_normal = None 
        _bt_resetar_ciclo_ga_st() 

        total_barras_operacionais = len(df_operacional)
        if total_barras_operacionais <= 1: 
            log_st_e_arquivo("BT: Número de barras operacionais insuficiente (<2).", "WARN")
            ss.bt_status_message = "BT: Concluído (poucos dados para operar)."
            ss.bt_esta_rodando = False; ss.bt_progresso = 100.0; return

        for i in range(1, total_barras_operacionais):
            if not ss.bt_esta_rodando: 
                log_st_e_arquivo("BT: Backtest interrompido externamente.", "INFO"); break

            ss.bt_progresso = (i / total_barras_operacionais) * 100.0

            barra_corrente_iteracao = df_operacional.iloc[i]
            timestamp_barra_corrente = barra_corrente_iteracao.name 

            if data_iteracao_atual != timestamp_barra_corrente.date():
                pnl_do_dia_atual = 0.0
                stop_diario_atingido_flag = False
                data_iteracao_atual = timestamp_barra_corrente.date()
                log_st_e_arquivo(f"BT: Novo dia {data_iteracao_atual}. PNL diário e stop resetados.", "DEBUG")

            if stop_diario_atingido_flag:
                if fechar_no_stop_diario_cfg_bt: 
                    if ss.bt_usar_ga_checkbox and ss.bt_ga_ciclo_ativo:
                        preco_saida_stop_dia_ga = barra_corrente_iteracao['open'] 
                        pnl_ciclo_ga_fechado_stop_dia = _bt_calcular_pnl_fechamento_ciclo_ga_st(preco_saida_stop_dia_ga, valor_por_ponto_ativo)

                        if ss.bt_ga_ciclo_info and ss.bt_ga_ciclo_info.get('ordens'): 
                            balanco_atual += pnl_ciclo_ga_fechado_stop_dia; pnl_do_dia_atual += pnl_ciclo_ga_fechado_stop_dia
                            lista_trades_executados.append({
                                'type': f"GA_{ss.bt_ga_ciclo_info['tipo_base']}",
                                'entry_price': ss.bt_ga_ciclo_info['preco_medio_pond'],
                                'volume': ss.bt_ga_ciclo_info['vol_total'],
                                'exit_price': preco_saida_stop_dia_ga,
                                'entry_time': ss.bt_ga_ciclo_info.get('entry_time', timestamp_barra_corrente), 
                                'exit_time': timestamp_barra_corrente,
                                'pnl': pnl_ciclo_ga_fechado_stop_dia,
                                'reason': 'GA Stop Diario BT'
                            })
                            lista_curva_equity.append({'time': timestamp_barra_corrente, 'balance': balanco_atual})
                            log_st_e_arquivo(f"BT GA: Ciclo fechado por Stop Diário Config @ {preco_saida_stop_dia_ga:.{digitos_ativo}f}, PNL: {pnl_ciclo_ga_fechado_stop_dia:.2f}", "DEBUG")
                        _bt_resetar_ciclo_ga_st()

                    elif trade_ativo_no_momento_normal: 
                        preco_saida_stop_dia_normal = barra_corrente_iteracao['open']
                        trade_ativo_no_momento_normal['exit_price'] = preco_saida_stop_dia_normal
                        trade_ativo_no_momento_normal['exit_time'] = timestamp_barra_corrente
                        trade_ativo_no_momento_normal['reason'] = 'Stop Diario BT'
                        if trade_ativo_no_momento_normal['type'] == 'BUY':
                             pnl_calculado_saida_normal = (preco_saida_stop_dia_normal - trade_ativo_no_momento_normal['entry_price']) * trade_ativo_no_momento_normal['volume'] * valor_por_ponto_ativo
                        else: # SELL
                             pnl_calculado_saida_normal = (trade_ativo_no_momento_normal['entry_price'] - preco_saida_stop_dia_normal) * trade_ativo_no_momento_normal['volume'] * valor_por_ponto_ativo

                        trade_ativo_no_momento_normal['pnl'] = pnl_calculado_saida_normal
                        lista_trades_executados.append(trade_ativo_no_momento_normal.copy()) 
                        balanco_atual += pnl_calculado_saida_normal; pnl_do_dia_atual += pnl_calculado_saida_normal
                        lista_curva_equity.append({'time': timestamp_barra_corrente, 'balance': balanco_atual})
                        log_st_e_arquivo(f"BT Normal: Trade fechado por Stop Diário Config @ {preco_saida_stop_dia_normal:.{digitos_ativo}f}, PNL: {pnl_calculado_saida_normal:.2f}", "DEBUG")
                        trade_ativo_no_momento_normal = None 
                continue 

            horario_operacao_ok_bt = verificar_horario_operacao_bt_st(hora_inicio_bt, hora_fim_bt, timestamp_barra_corrente)
            if not horario_operacao_ok_bt:
                if ss.bt_usar_ga_checkbox and ss.bt_ga_ciclo_ativo:
                    pnl_ciclo_ga_gerenciado_fh = _bt_gerenciar_ciclo_ga_sw_sl_st(barra_corrente_iteracao, valor_por_ponto_ativo, ponto_ativo, digitos_ativo, lista_trades_executados, lista_curva_equity, balanco_atual, timestamp_barra_corrente)
                    if pnl_ciclo_ga_gerenciado_fh is not None:
                        balanco_atual += pnl_ciclo_ga_gerenciado_fh; pnl_do_dia_atual += pnl_ciclo_ga_gerenciado_fh
                elif trade_ativo_no_momento_normal:
                    trade_ativo_no_momento_normal, pnl_calculado_saida_normal_fh = _bt_gerenciar_trade_normal_sl_tp_st(trade_ativo_no_momento_normal, barra_corrente_iteracao, valor_por_ponto_ativo, lista_trades_executados, lista_curva_equity, balanco_atual, timestamp_barra_corrente)
                    if pnl_calculado_saida_normal_fh is not None:
                        balanco_atual += pnl_calculado_saida_normal_fh; pnl_do_dia_atual += pnl_calculado_saida_normal_fh
                continue 

            if ss.bt_usar_ga_checkbox: 
                if ss.bt_ga_ciclo_ativo: 
                    pnl_ciclo_ga_gerenciado = _bt_gerenciar_ciclo_ga_sw_sl_st(barra_corrente_iteracao, valor_por_ponto_ativo, ponto_ativo, digitos_ativo, lista_trades_executados, lista_curva_equity, balanco_atual, timestamp_barra_corrente)
                    if pnl_ciclo_ga_gerenciado is not None: 
                        balanco_atual += pnl_ciclo_ga_gerenciado; pnl_do_dia_atual += pnl_ciclo_ga_gerenciado
                    else: 
                        if not stop_diario_atingido_flag : 
                             _bt_adicionar_nivel_ga_st(barra_corrente_iteracao, ponto_ativo, digitos_ativo, timestamp_barra_corrente)

                else: 
                    if trade_ativo_no_momento_normal: continue 
                    if stop_diario_atingido_flag: continue 

                    df_para_sinal_ga_bt = df_operacional.iloc[:i] 
                    sinal_para_iniciar_ga_bt = _bt_gerar_sinal_ichimoku_st(df_para_sinal_ga_bt, ichimoku_params_bt_cfg)
                    if sinal_para_iniciar_ga_bt:
                        preco_entrada_ciclo_ga_simulado = barra_corrente_iteracao['open']
                        _bt_iniciar_ciclo_ga_st(sinal_para_iniciar_ga_bt['tipo'], preco_entrada_ciclo_ga_simulado, timestamp_barra_corrente, ss.bt_ga_vol_inicial, ponto_ativo, digitos_ativo)

            else: 
                if ss.bt_ga_ciclo_ativo: _bt_resetar_ciclo_ga_st() 

                if trade_ativo_no_momento_normal: 
                    trade_ativo_no_momento_normal, pnl_calculado_saida_normal = _bt_gerenciar_trade_normal_sl_tp_st(trade_ativo_no_momento_normal, barra_corrente_iteracao, valor_por_ponto_ativo, lista_trades_executados, lista_curva_equity, balanco_atual, timestamp_barra_corrente)
                    if pnl_calculado_saida_normal is not None:
                        balanco_atual += pnl_calculado_saida_normal; pnl_do_dia_atual += pnl_calculado_saida_normal

                else: 
                    if stop_diario_atingido_flag: continue 

                    df_para_sinal_normal_bt = df_operacional.iloc[:i] 
                    sinal_gerado_bt_normal = _bt_gerar_sinal_ichimoku_st(df_para_sinal_normal_bt, ichimoku_params_bt_cfg)

                    if sinal_gerado_bt_normal:
                        preco_entrada_trade_normal_base = barra_corrente_iteracao['open'] 
                        preco_final_entrada_normal_efetivo = preco_entrada_trade_normal_base

                        if delay_entrada_pts_bt > 0:
                            preco_ref_sinal_delay_normal = df_operacional.iloc[i-1][sinal_gerado_bt_normal.get('trigger_price_ref_col', 'close')]

                            if sinal_gerado_bt_normal['tipo'] == 'BUY':
                                preco_gatilho_com_delay_normal = round(preco_ref_sinal_delay_normal + delay_entrada_pts_bt * ponto_ativo, digitos_ativo)
                                if preco_entrada_trade_normal_base > preco_gatilho_com_delay_normal: continue
                                preco_final_entrada_normal_efetivo = preco_gatilho_com_delay_normal
                            else: # SELL
                                preco_gatilho_com_delay_normal = round(preco_ref_sinal_delay_normal - delay_entrada_pts_bt * ponto_ativo, digitos_ativo)
                                if preco_entrada_trade_normal_base < preco_gatilho_com_delay_normal: continue
                                preco_final_entrada_normal_efetivo = preco_gatilho_com_delay_normal

                        sl_preco_abs_normal, tp_preco_abs_normal = None, None
                        if sl_config_pts_normal > 0:
                            sl_preco_abs_normal = round(preco_final_entrada_normal_efetivo - sl_config_pts_normal * ponto_ativo, digitos_ativo) if sinal_gerado_bt_normal['tipo'] == 'BUY' else round(preco_final_entrada_normal_efetivo + sl_config_pts_normal * ponto_ativo, digitos_ativo)
                        if tp_config_pts_normal > 0:
                            tp_preco_abs_normal = round(preco_final_entrada_normal_efetivo + tp_config_pts_normal * ponto_ativo, digitos_ativo) if sinal_gerado_bt_normal['tipo'] == 'BUY' else round(preco_final_entrada_normal_efetivo - tp_config_pts_normal * ponto_ativo, digitos_ativo)

                        if stop_financeiro_por_trade_cfg_bt > 0 and sl_config_pts_normal > 0 and valor_por_ponto_ativo > 0:
                            perda_com_sl_original_em_reais_normal = sl_config_pts_normal * valor_por_ponto_ativo * volume_base_bt_normal
                            if perda_com_sl_original_em_reais_normal > stop_financeiro_por_trade_cfg_bt:
                                novos_pontos_sl_calc_normal = (stop_financeiro_por_trade_cfg_bt / (valor_por_ponto_ativo * volume_base_bt_normal)) if (valor_por_ponto_ativo * volume_base_bt_normal) > 0 else 0
                                if novos_pontos_sl_calc_normal > 0:
                                    sl_preco_abs_normal = round(preco_final_entrada_normal_efetivo - novos_pontos_sl_calc_normal * ponto_ativo, digitos_ativo) if sinal_gerado_bt_normal['tipo'] == 'BUY' else round(preco_final_entrada_normal_efetivo + novos_pontos_sl_calc_normal * ponto_ativo, digitos_ativo)
                                    log_st_e_arquivo(f"BT Normal: SL ajustado por stop financeiro. Orig: {sl_config_pts_normal}pts, Novo: {novos_pontos_sl_calc_normal:.2f}pts. Novo SL Price: {sl_preco_abs_normal:.{digitos_ativo}f}", "DEBUG")

                        trade_ativo_no_momento_normal = {
                            'type': sinal_gerado_bt_normal['tipo'],
                            'entry_price': preco_final_entrada_normal_efetivo,
                            'volume': volume_base_bt_normal,
                            'sl_price': sl_preco_abs_normal,
                            'tp_price': tp_preco_abs_normal,
                            'entry_time': timestamp_barra_corrente,
                            'initial_sl_points': sl_config_pts_normal 
                        }
                        log_st_e_arquivo(f"BT Normal: Novo trade {trade_ativo_no_momento_normal['type']} @ {trade_ativo_no_momento_normal['entry_price']:.{digitos_ativo}f}, SL: {sl_preco_abs_normal if sl_preco_abs_normal else 'N/A'}, TP: {tp_preco_abs_normal if tp_preco_abs_normal else 'N/A'}", "DEBUG")


            if stop_diario_total_financeiro_cfg_bt > 0 and pnl_do_dia_atual <= -stop_diario_total_financeiro_cfg_bt:
                if not stop_diario_atingido_flag: 
                    log_st_e_arquivo(f"BT: STOP DIÁRIO FINANCEIRO ATINGIDO em {data_iteracao_atual}! PNL Dia: {pnl_do_dia_atual:.2f} (Limite: {-stop_diario_total_financeiro_cfg_bt:.2f})", "WARN")
                stop_diario_atingido_flag = True
                
        if not df_operacional.empty: 
            ultimo_fechamento = df_operacional.iloc[-1]['close']
            timestamp_ultimo_fechamento = df_operacional.index[-1]

            if ss.bt_usar_ga_checkbox and ss.bt_ga_ciclo_ativo:
                if ss.bt_ga_ciclo_info and ss.bt_ga_ciclo_info.get('ordens'): 
                    pnl_ciclo_ga_final = _bt_calcular_pnl_fechamento_ciclo_ga_st(ultimo_fechamento, valor_por_ponto_ativo)
                    balanco_atual += pnl_ciclo_ga_final
                    lista_trades_executados.append({
                        'type': f"GA_{ss.bt_ga_ciclo_info['tipo_base']}",
                        'entry_price': ss.bt_ga_ciclo_info['preco_medio_pond'],
                        'volume': ss.bt_ga_ciclo_info['vol_total'],
                        'exit_price': ultimo_fechamento,
                        'entry_time': ss.bt_ga_ciclo_info.get('entry_time', timestamp_ultimo_fechamento),
                        'exit_time': timestamp_ultimo_fechamento,
                        'pnl': pnl_ciclo_ga_final,
                        'reason': 'GA Fim Backtest'
                    })
                    lista_curva_equity.append({'time': timestamp_ultimo_fechamento, 'balance': balanco_atual})
                    log_st_e_arquivo(f"BT GA: Ciclo finalizado no fim do backtest. PNL: {pnl_ciclo_ga_final:.2f}", "INFO")
                _bt_resetar_ciclo_ga_st()

            elif trade_ativo_no_momento_normal:
                pnl_final_trade_aberto_normal = 0.0
                if trade_ativo_no_momento_normal['type'] == 'BUY':
                    pnl_final_trade_aberto_normal = (ultimo_fechamento - trade_ativo_no_momento_normal['entry_price']) * trade_ativo_no_momento_normal['volume'] * valor_por_ponto_ativo
                else: # SELL
                    pnl_final_trade_aberto_normal = (trade_ativo_no_momento_normal['entry_price'] - ultimo_fechamento) * trade_ativo_no_momento_normal['volume'] * valor_por_ponto_ativo

                balanco_atual += pnl_final_trade_aberto_normal
                trade_ativo_no_momento_normal['exit_price'] = ultimo_fechamento
                trade_ativo_no_momento_normal['exit_time'] = timestamp_ultimo_fechamento
                trade_ativo_no_momento_normal['pnl'] = pnl_final_trade_aberto_normal
                trade_ativo_no_momento_normal['reason'] = 'Fim Backtest'
                lista_trades_executados.append(trade_ativo_no_momento_normal)
                lista_curva_equity.append({'time': timestamp_ultimo_fechamento, 'balance': balanco_atual})
                log_st_e_arquivo(f"BT Normal: Trade finalizado no fim do backtest. PNL: {pnl_final_trade_aberto_normal:.2f}", "INFO")

        ss.bt_trades_realizados_df = pd.DataFrame(lista_trades_executados)
        if lista_curva_equity:
            ss.bt_equity_curve_df = pd.DataFrame(lista_curva_equity)
        else: 
            ss.bt_equity_curve_df = pd.DataFrame([{'time': start_dt_obj, 'balance': initial_balance_bt}])

        pnl_total_calculado = balanco_atual - initial_balance_bt
        num_total_trades = len(lista_trades_executados)
        num_trades_vencedores = sum(1 for t in lista_trades_executados if t.get('pnl', 0) > 0)
        taxa_de_acerto = (num_trades_vencedores / num_total_trades * 100) if num_total_trades > 0 else 0.0

        ss.bt_pnl_total_str = f"PNL Total: R$ {pnl_total_calculado:.2f}"
        ss.bt_num_trades_str = f"Trades: {num_total_trades}"
        ss.bt_win_rate_str = f"Win Rate: {taxa_de_acerto:.2f}%"
        log_st_e_arquivo(f"BT Concluído (cálculos). PNL: {pnl_total_calculado:.2f}, Trades: {num_total_trades}, WinRate: {taxa_de_acerto:.2f}%", "INFO")

    except Exception as e_bt_thread_geral:
        log_st_e_arquivo(f"Erro GERAL na Thread de Backtest: {e_bt_thread_geral}", "CRITICAL")
        logger.exception("Detalhes da exceção na thread de backtest:") 
        ss.bt_status_message = f"Erro BT Grave: {str(e_bt_thread_geral)[:100]}" 
    finally:
        ss.bt_esta_rodando = False; ss.bt_progresso = 100.0
        if IS_ON_STREAMLIT_CLOUD and ("Não funcional em Cloud" in ss.bt_status_message or "sem dados MT5" in ss.bt_status_message):
            pass 
        elif "Erro" in ss.bt_status_message or "Falha" in ss.bt_status_message:
            pass 
        else:
            ss.bt_status_message = "BT: Concluído." if not ss.bt_trades_realizados_df.empty else "BT: Concluído (sem trades)."
        log_st_e_arquivo(f"Thread BT finalizada. Status final da thread: {ss.bt_status_message}", "INFO")


def acionar_iniciar_backtest_st():
    ss = st.session_state
    if ss.bt_esta_rodando:
        st.warning("Backtest já está em andamento."); return

    log_st_e_arquivo("Comando Iniciar Backtest recebido.", "INFO")
    ss.bt_esta_rodando = True; ss.bt_status_message = "Iniciando Backtest..."; ss.bt_progresso = 0.0
    ss.bt_trades_realizados_df = pd.DataFrame()
    ss.bt_equity_curve_df = pd.DataFrame(columns=['time', 'balance'])
    ss.bt_pnl_total_str = "Calculando..."; ss.bt_num_trades_str = "Calculando..."; ss.bt_win_rate_str = "Calculando..."

    thread_bt_atual = ss.get("bt_thread_referencia")
    if thread_bt_atual is None or not thread_bt_atual.is_alive():
        nova_thread_bt = threading.Thread(target=funcao_alvo_thread_backtest, name="BacktestWorker", daemon=True)
        ss.bt_thread_referencia = nova_thread_bt
        nova_thread_bt.start()
    else:
        log_st_e_arquivo("Thread de Backtest anterior ainda parece ativa. Cancelando novo início.", "WARN")
        ss.bt_esta_rodando = False; st.error("Erro: Thread BT anterior ativa. Aguarde ou reinicie a aplicação.")

# --- Interface Streamlit ---
st.set_page_config(page_title="Robô Trader ST (com GA - Corrigido)", layout="wide")
st.title("🤖 Robô Trader - Ichimoku com Gradiente Averaging (Streamlit)")

ss = st.session_state
if IS_ON_STREAMLIT_CLOUD:
    st.warning("⚠️ MODO DEMONSTRAÇÃO (STREAMLIT CLOUD) ⚠️\n\nA conexão real com MetaTrader 5 e as operações de trading live não são funcionais neste ambiente. O backtest não buscará dados reais do MT5 (usará dados simulados ou vazios).")

with st.sidebar:
    st.header("🔑 Conexão MT5")
    ss.mt5_login_input = st.text_input("Login MT5", value=ss.mt5_login_input, key="p3ga_corr_mt5_login", disabled=IS_ON_STREAMLIT_CLOUD or ss.mt5_esta_conectado)
    ss.mt5_password_input = st.text_input("Senha MT5", type="password", value=ss.mt5_password_input, key="p3ga_corr_mt5_pass", disabled=IS_ON_STREAMLIT_CLOUD or ss.mt5_esta_conectado)
    ss.mt5_server_input = st.text_input("Servidor MT5", value=ss.mt5_server_input, key="p3ga_corr_mt5_server", disabled=IS_ON_STREAMLIT_CLOUD or ss.mt5_esta_conectado)

    c1_conn, c2_conn = st.columns(2)
    if c1_conn.button("Conectar MT5", key="p3ga_corr_connect_btn", use_container_width=True, disabled=ss.mt5_esta_conectado or IS_ON_STREAMLIT_CLOUD):
        if not IS_ON_STREAMLIT_CLOUD:
            conectado = conectar_mt5_st(ss.mt5_login_input, ss.mt5_password_input, ss.mt5_server_input)
            if conectado: st.rerun()

    if c2_conn.button("Desconectar MT5", key="p3ga_corr_disconnect_btn", use_container_width=True, disabled=not ss.mt5_esta_conectado or IS_ON_STREAMLIT_CLOUD):
        if not IS_ON_STREAMLIT_CLOUD:
            if ss.robo_esta_ligado:
                acionar_desligar_robo_live()
                time.sleep(ss.intervalo_robo_segundos + 1)
            desligar_mt5_st()
        st.rerun()

    st.markdown("---")
    st.header("⚙️ Controle Robô (Live)")
    controles_live_disabled = (not ss.mt5_esta_conectado and not IS_ON_STREAMLIT_CLOUD) or IS_ON_STREAMLIT_CLOUD

    c1_live_ctrl, c2_live_ctrl = st.columns(2)
    if c1_live_ctrl.button("▶️ Ligar Robô Live", key="p3ga_corr_start_live_btn", use_container_width=True,
                           disabled=ss.robo_esta_ligado or controles_live_disabled):
        if not IS_ON_STREAMLIT_CLOUD: acionar_ligar_robo_live()
        st.rerun()

    if c2_live_ctrl.button("⏹️ Desligar Robô Live", key="p3ga_corr_stop_live_btn", use_container_width=True,
                           disabled=not ss.robo_esta_ligado or IS_ON_STREAMLIT_CLOUD):
        acionar_desligar_robo_live()
        st.rerun()

    with st.expander("🔧 Configs Trading Live", expanded=False):
        if IS_ON_STREAMLIT_CLOUD: st.caption("Configs Live são demonstrativas no Streamlit Cloud.")
        configs_live_disabled_runtime = (ss.robo_esta_ligado and not IS_ON_STREAMLIT_CLOUD) or IS_ON_STREAMLIT_CLOUD

        ss.ativo_principal_live = st.text_input("Ativo Live", value=ss.ativo_principal_live, key="p3ga_corr_ativo_live", disabled=configs_live_disabled_runtime)
        ss.timeframe_live = st.selectbox("TF Live (min)", [1,5,15,30,60,240,1440], index=[1,5,15,30,60,240,1440].index(ss.timeframe_live), key="p3ga_corr_tf_live", disabled=configs_live_disabled_runtime)
        ss.intervalo_robo_segundos = st.number_input("Intervalo Loop Live (s)", min_value=1, value=ss.intervalo_robo_segundos, step=1, key="p3ga_corr_intervalo_live", disabled=configs_live_disabled_runtime)
        ss.hora_inicio_live = st.text_input("Hora Início Live (HH:MM)", value=ss.hora_inicio_live, key="p3ga_corr_h_ini_live", disabled=configs_live_disabled_runtime)
        ss.hora_fim_live = st.text_input("Hora Fim Live (HH:MM)", value=ss.hora_fim_live, key="p3ga_corr_h_fim_live", disabled=configs_live_disabled_runtime)

        st.caption("Ichimoku Live (Sinal Base)")
        c1l_cfg,c2l_cfg,c3l_cfg = st.columns(3)
        ss.tenkan_period_live = c1l_cfg.number_input("Tenkan", value=ss.tenkan_period_live, key="p3ga_corr_tenkan_live", disabled=configs_live_disabled_runtime, min_value=1)
        ss.kijun_period_live = c2l_cfg.number_input("Kijun", value=ss.kijun_period_live, key="p3ga_corr_kijun_live", disabled=configs_live_disabled_runtime, min_value=1)
        ss.senkou_b_period_live = c3l_cfg.number_input("SenkouB", value=ss.senkou_b_period_live, key="p3ga_corr_senkoub_live", disabled=configs_live_disabled_runtime, min_value=1)
        ss.delay_entry_points_live = st.number_input("Delay Entrada Live (pts)", value=float(ss.delay_entry_points_live), format="%.1f", key="p3ga_corr_delay_live", disabled=configs_live_disabled_runtime, min_value=0.0)

        st.markdown("---")
        st.subheader("Gradiente Averaging (Live)")
        ss.ga_live_ativo_checkbox = st.checkbox("Ativar Gradiente Averaging (Live)", value=ss.ga_live_ativo_checkbox, key="p3ga_corr_ga_ativo_live_cb", disabled=configs_live_disabled_runtime)
        if ss.ga_live_ativo_checkbox:
            ss.ga_live_vol_inicial = st.number_input("Vol. Inicial GA Live", value=ss.ga_live_vol_inicial, step=0.01, format="%.2f", key="p3ga_corr_ga_vol_ini_live", disabled=configs_live_disabled_runtime, min_value=0.01)
            ss.ga_live_dist_base_pts = st.number_input("Dist. Níveis GA Live (pts)", value=ss.ga_live_dist_base_pts, key="p3ga_corr_ga_dist_live", disabled=configs_live_disabled_runtime, min_value=1)
            ss.ga_live_vol_nivel_sub = st.number_input("Vol. Nível Sub. GA Live", value=ss.ga_live_vol_nivel_sub, step=0.01, format="%.2f", key="p3ga_corr_ga_vol_sub_live", disabled=configs_live_disabled_runtime, min_value=0.01)
            ss.ga_live_max_niveis = st.number_input("Max Níveis GA Live", value=ss.ga_live_max_niveis, step=1, key="p3ga_corr_ga_max_niv_live", disabled=configs_live_disabled_runtime, min_value=1)
            ss.ga_live_pnl_sw_ciclo_pts = st.number_input("Stop Win Ciclo GA Live (pts)", value=ss.ga_live_pnl_sw_ciclo_pts, step=1, key="p3ga_corr_ga_sw_ciclo_live", disabled=configs_live_disabled_runtime, min_value=0)
        else:
            st.caption("Trade Normal (Live - Se GA Desativado)")
            ss.volume_live = st.number_input("Volume Normal Live", value=ss.volume_live, step=0.01, format="%.2f", key="p3ga_corr_vol_live_normal", disabled=configs_live_disabled_runtime, min_value=0.01)
            c1rl_cfg,c2rl_cfg = st.columns(2)
            ss.stop_loss_pts_live = c1rl_cfg.number_input("SL Normal (pts)", value=ss.stop_loss_pts_live, format="%.1f", key="p3ga_corr_sl_live_normal", disabled=configs_live_disabled_runtime, min_value=0.0)
            ss.stop_win_pts_live = c2rl_cfg.number_input("TP Normal (pts)", value=ss.stop_win_pts_live, format="%.1f", key="p3ga_corr_tp_live_normal", disabled=configs_live_disabled_runtime, min_value=0.0)

        st.markdown("---")
        st.caption("Risco Geral (Live)")
        ss.max_loss_trade_financeiro_live = st.number_input("Max Loss/Trade ou Ciclo GA (R$)", value=ss.max_loss_trade_financeiro_live, format="%.2f", key="p3ga_corr_stop_trade_ciclo_live", disabled=configs_live_disabled_runtime, min_value=0.0)
        ss.daily_total_stop_loss_financeiro_live = st.number_input("Stop Diário Total Live (R$)", value=ss.daily_total_stop_loss_financeiro_live, format="%.2f", key="p3ga_corr_stop_dia_live", disabled=configs_live_disabled_runtime, min_value=0.0)

    st.markdown("---")
    st.header("📊 Backtesting")
    configs_bt_disabled_runtime = ss.bt_esta_rodando

    with st.expander("Parâmetros do Backtest", expanded=True):
        ss.bt_ativo = st.text_input("Ativo BT", value=ss.bt_ativo, key="p3ga_corr_bt_ativo", disabled=configs_bt_disabled_runtime)
        c1_bt_dt,c2_bt_dt = st.columns(2)
        ss.bt_start_date = c1_bt_dt.date_input("Data Início BT", value=ss.bt_start_date, key="p3ga_corr_bt_start_dt", max_value=datetime.date.today() - datetime.timedelta(days=1), disabled=configs_bt_disabled_runtime)
        ss.bt_end_date = c2_bt_dt.date_input("Data Fim BT", value=ss.bt_end_date, key="p3ga_corr_bt_end_dt", max_value=datetime.date.today() - datetime.timedelta(days=1), disabled=configs_bt_disabled_runtime)
        ss.bt_timeframe = st.selectbox("TF BT (min)", [1,5,15,30,60,240,1440], index=[1,5,15,30,60,240,1440].index(ss.bt_timeframe), key="p3ga_corr_bt_tf", disabled=configs_bt_disabled_runtime)
        ss.bt_initial_balance = st.number_input("Saldo Inicial BT (R$)", value=ss.bt_initial_balance, step=100.0, format="%.2f", key="p3ga_corr_bt_bal", disabled=configs_bt_disabled_runtime, min_value=1.0)
        ss.bt_delay_entry_pts = st.number_input("Delay Entrada BT (pts)", value=float(ss.bt_delay_entry_pts), format="%.1f", step=1.0, key="p3ga_corr_bt_delay", disabled=configs_bt_disabled_runtime, min_value=0.0)
        ss.bt_hora_inicio_op = st.text_input("Hora Início Op BT (HH:MM)", value=ss.bt_hora_inicio_op, key="p3ga_corr_bt_h_ini", disabled=configs_bt_disabled_runtime)
        ss.bt_hora_fim_op = st.text_input("Hora Fim Op BT (HH:MM)", value=ss.bt_hora_fim_op, key="p3ga_corr_bt_h_fim", disabled=configs_bt_disabled_runtime)

        st.caption("Ichimoku BT (Sinal Base)")
        c1b_cfg,c2b_cfg,c3b_cfg = st.columns(3)
        ss.bt_tenkan_period = c1b_cfg.number_input("Tenkan BT", value=ss.bt_tenkan_period, key="p3ga_corr_bt_tenkan", disabled=configs_bt_disabled_runtime, min_value=1)
        ss.bt_kijun_period = c2b_cfg.number_input("Kijun BT", value=ss.bt_kijun_period, key="p3ga_corr_bt_kijun", disabled=configs_bt_disabled_runtime, min_value=1)
        ss.bt_senkou_b_period = c3b_cfg.number_input("SenkouB BT", value=ss.bt_senkou_b_period, key="p3ga_corr_bt_senkoub", disabled=configs_bt_disabled_runtime, min_value=1)

        st.markdown("---")
        st.subheader("Gradiente Averaging (Backtest)")
        ss.bt_usar_ga_checkbox = st.checkbox("Usar Gradiente Averaging (Backtest)", value=ss.bt_usar_ga_checkbox, key="p3ga_corr_ga_ativo_bt_cb", disabled=configs_bt_disabled_runtime)
        if ss.bt_usar_ga_checkbox:
            ss.bt_ga_vol_inicial = st.number_input("Vol. Inicial GA BT", value=ss.bt_ga_vol_inicial, step=0.01, format="%.2f", key="p3ga_corr_ga_vol_ini_bt", disabled=configs_bt_disabled_runtime, min_value=0.01)
            ss.bt_ga_dist_base_pts = st.number_input("Dist. Níveis GA BT (pts)", value=ss.bt_ga_dist_base_pts, key="p3ga_corr_ga_dist_bt", disabled=configs_bt_disabled_runtime, min_value=1)
            ss.bt_ga_vol_nivel_sub = st.number_input("Vol. Nível Sub. GA BT", value=ss.bt_ga_vol_nivel_sub, step=0.01, format="%.2f", key="p3ga_corr_ga_vol_sub_bt", disabled=configs_bt_disabled_runtime, min_value=0.01)
            ss.bt_ga_max_niveis = st.number_input("Max Níveis GA BT", value=ss.bt_ga_max_niveis, step=1, key="p3ga_corr_ga_max_niv_bt", disabled=configs_bt_disabled_runtime, min_value=1)
            ss.bt_ga_pnl_sw_ciclo_pts = st.number_input("Stop Win Ciclo GA BT (pts)", value=ss.bt_ga_pnl_sw_ciclo_pts, step=1, key="p3ga_corr_ga_sw_ciclo_bt", disabled=configs_bt_disabled_runtime, min_value=0)
        else:
            st.caption("Trade Normal (Backtest - Se GA Desativado)")
            ss.bt_volume = st.number_input("Volume Normal BT", value=ss.bt_volume, step=0.01, format="%.2f", key="p3ga_corr_bt_vol_normal", disabled=configs_bt_disabled_runtime, min_value=0.01)
            c1rb_cfg,c2rb_cfg = st.columns(2)
            ss.bt_sl_pts = c1rb_cfg.number_input("SL Normal BT (pts)", value=ss.bt_sl_pts, format="%.1f", key="p3ga_corr_bt_sl_normal", disabled=configs_bt_disabled_runtime, min_value=0.0)
            ss.bt_tp_pts = c2rb_cfg.number_input("TP Normal BT (pts)", value=ss.bt_tp_pts, format="%.1f", key="p3ga_corr_bt_tp_normal", disabled=configs_bt_disabled_runtime, min_value=0.0)

        st.markdown("---")
        st.caption("Risco Geral (Backtest)")
        ss.bt_stop_financeiro_trade = st.number_input("Stop Trade/Ciclo GA BT (R$)", value=ss.bt_stop_financeiro_trade, format="%.2f", key="p3ga_corr_bt_stop_trade_ciclo", disabled=configs_bt_disabled_runtime, min_value=0.0)
        ss.bt_stop_diario_total = st.number_input("Stop Diário Total BT (R$)", value=ss.bt_stop_diario_total, format="%.2f", key="p3ga_corr_bt_stop_dia", disabled=configs_bt_disabled_runtime, min_value=0.0)
        ss.bt_fechar_pos_stop_diario = st.checkbox("Fechar Posições ao Atingir Stop Diário (BT)", value=ss.bt_fechar_pos_stop_diario, key="p3ga_corr_bt_fechar_stop", disabled=configs_bt_disabled_runtime)

        if st.button("🚀 Iniciar Backtest", key="p3ga_corr_bt_run_btn", use_container_width=True, disabled=ss.bt_esta_rodando):
            acionar_iniciar_backtest_st(); st.rerun()

        st.progress(int(ss.bt_progresso))
        st.caption(ss.bt_status_message)

# --- Área Principal da UI ---
st.subheader("ℹ️ Status Robô Live")
col_s1_main, col_s2_main = st.columns(2)
with col_s1_main:
    status_cor_live_ui = "grey"
    icon_live_ui = "ℹ️"
    status_texto_ui = ss.status_interface

    if IS_ON_STREAMLIT_CLOUD:
        status_cor_live_ui = "orange"
        icon_live_ui = "☁️"
        status_texto_ui = "Robô Live Desabilitado (Cloud)"
    elif ss.robo_esta_ligado:
        status_cor_live_ui = "green"
        icon_live_ui = "🚀"
        if "Stop Diário" in status_texto_ui: status_cor_live_ui = "orange"
        if "Erro" in status_texto_ui : status_cor_live_ui = "red"
    else:
        status_cor_live_ui = "red"
        icon_live_ui = "🛑"
        if "Erro" in status_texto_ui : status_cor_live_ui = "red"
        elif "Desligando" not in status_texto_ui and "Desligado" not in status_texto_ui:
            status_texto_ui = "Robô Desligado"


    st.markdown(f"**Robô Live:** <span style='color:{status_cor_live_ui};'>{icon_live_ui} {status_texto_ui}</span>", unsafe_allow_html=True)

with col_s2_main:
    conn_cor_live_ui = "red"
    icon_conn_ui = "⚠️"
    conexao_texto_ui = ss.conexao_mt5_interface

    if IS_ON_STREAMLIT_CLOUD:
        conn_cor_live_ui = "orange"
        conexao_texto_ui = "MT5 Desabilitado (Cloud)"
    elif ss.mt5_esta_conectado:
        conn_cor_live_ui = "green"
        icon_conn_ui = "🔗"

    st.markdown(f"**MetaTrader 5:** <span style='color:{conn_cor_live_ui};'>{icon_conn_ui} {conexao_texto_ui}</span>", unsafe_allow_html=True)


if not IS_ON_STREAMLIT_CLOUD and ss.get("mt5_esta_conectado", False) and ss.get("robo_esta_ligado", False):
    atualizar_display_info_robo_st()

display_pos_live_final = ss.posicao_info_live
if IS_ON_STREAMLIT_CLOUD: display_pos_live_final = "N/A (Cloud)"

display_pnl_live = ss.pnl_info_live
if IS_ON_STREAMLIT_CLOUD: display_pnl_live = "PNL Dia: N/A / PNL Op.: N/A (Cloud)"

display_price_live = ss.tick_price_display_live
if IS_ON_STREAMLIT_CLOUD: display_price_live = f"Preço ({ss.ativo_principal_live}): N/A (Cloud)"

st.markdown(f"**Posição Live:** `{display_pos_live_final}` | **PNL Live:** `{display_pnl_live}` | **Preço Live:** `{display_price_live}`")
st.markdown("---"); st.subheader("📈 Resultados do Backtest")

show_bt_results_placeholder = ss.bt_esta_rodando or \
                              "Concluído" in ss.bt_status_message or \
                              "Erro" in ss.bt_status_message or \
                              "Não funcional" in ss.bt_status_message or \
                              (not ss.bt_trades_realizados_df.empty)

if show_bt_results_placeholder:
    c1_bt_res, c2_bt_res, c3_bt_res = st.columns(3)
    pnl_val = "0.00";
    try: pnl_val = ss.bt_pnl_total_str.split(": R$ ")[-1].strip() if isinstance(ss.bt_pnl_total_str, str) and "R$" in ss.bt_pnl_total_str else ss.bt_pnl_total_str
    except: pnl_val = str(ss.bt_pnl_total_str)
    c1_bt_res.metric("PNL Total BT", value=f"R$ {pnl_val}")

    trades_val = "0";
    try: trades_val = ss.bt_num_trades_str.split(": ")[-1].strip() if isinstance(ss.bt_num_trades_str, str) and ": " in ss.bt_num_trades_str else ss.bt_num_trades_str
    except: trades_val = str(ss.bt_num_trades_str)
    c2_bt_res.metric("Trades BT", value=trades_val)

    winrate_val = "0.00%";
    try: winrate_val = ss.bt_win_rate_str.split(": ")[-1].strip() if isinstance(ss.bt_win_rate_str, str) and ": " in ss.bt_win_rate_str else ss.bt_win_rate_str
    except: winrate_val = str(ss.bt_win_rate_str)
    c3_bt_res.metric("Win Rate BT", value=winrate_val)

    if not ss.bt_equity_curve_df.empty and 'balance' in ss.bt_equity_curve_df.columns and 'time' in ss.bt_equity_curve_df.columns:
        df_chart = ss.bt_equity_curve_df.copy()
        if pd.api.types.is_datetime64_any_dtype(df_chart['time']):
             df_chart = df_chart.set_index('time')

        if not df_chart.empty and 'balance' in df_chart.columns and len(df_chart) > 1:
            st.line_chart(df_chart['balance'], height=300)
        elif not df_chart.empty and 'balance' in df_chart.columns and len(df_chart) == 1:
             st.caption("Curva de equity com apenas um ponto (saldo inicial).")
        else:
             st.caption("Dados da curva de equity insuficientes ou mal formatados para exibir gráfico.")
    else:
        st.caption("Curva de equity não disponível ou vazia.")

    if not ss.bt_trades_realizados_df.empty:
        st.write("Histórico de Trades do Backtest:")
        df_display_bt = ss.bt_trades_realizados_df.copy()
        for col_time_bt in ['entry_time', 'exit_time']:
            if col_time_bt in df_display_bt.columns and not df_display_bt[col_time_bt].empty:
                try:
                    df_display_bt[col_time_bt] = pd.to_datetime(df_display_bt[col_time_bt]).dt.strftime('%Y-%m-%d %H:%M')
                except: pass
        if 'pnl' in df_display_bt.columns:
            try: df_display_bt['pnl'] = df_display_bt['pnl'].round(2)
            except: pass

        cols_para_mostrar_bt = ['entry_time', 'type', 'volume', 'entry_price', 'exit_time', 'exit_price', 'pnl', 'reason']
        cols_existentes_bt = [c for c in cols_para_mostrar_bt if c in df_display_bt.columns]

        if cols_existentes_bt:
            st.dataframe(df_display_bt[cols_existentes_bt], height=300, use_container_width=True)
        else:
            st.caption("Dados de trades do BT não disponíveis ou colunas ausentes.")
    elif ss.bt_esta_rodando:
        st.caption("Aguardando resultados do backtest...")
    elif "Concluído" in ss.bt_status_message and "(sem trades)" in ss.bt_status_message :
         st.info("Backtest concluído sem realizar trades.")

else:
    st.info("Nenhum backtest executado ou sem resultados para exibir. Configure e inicie um backtest na barra lateral.")

st.markdown("---"); st.subheader("📜 Logs de Eventos Gerais")
log_texto_area_principal = "\n".join(reversed(ss.logs_para_display))
st.text_area("Logs Gerais", value=log_texto_area_principal, height=250, key="p3ga_corr_log_area_main", disabled=True)

REFRESH_INTERVALO_P3GA_CORR = 2.5
if 'last_ui_refresh_p3ga_corr' not in ss:
    ss.last_ui_refresh_p3ga_corr = time.time()

deve_fazer_auto_refresh = False
if ss.get("robo_esta_ligado", False) and not IS_ON_STREAMLIT_CLOUD:
    deve_fazer_auto_refresh = True
if ss.get("bt_esta_rodando", False):
    deve_fazer_auto_refresh = True

if deve_fazer_auto_refresh:
    if (time.time() - ss.last_ui_refresh_p3ga_corr > REFRESH_INTERVALO_P3GA_CORR):
        ss.last_ui_refresh_p3ga_corr = time.time()
        st.rerun()
else:
    ss.last_ui_refresh_p3ga_corr = time.time()