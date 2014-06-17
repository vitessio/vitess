package com.github.youtube.vitess.jdbc.vtocc;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.inject.Provider;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;

import com.github.youtube.vitess.jdbc.vtocc.QueryService.Session;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionInfo;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SessionParams;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.SqlQuery;
import com.github.youtube.vitess.jdbc.vtocc.QueryService.TransactionInfo;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.MockitoAnnotations;
import org.mockito.MockitoAnnotations.Mock;

import java.sql.SQLException;

/**
 * Tests {@link VtoccTransactionHandler}.
 */
@RunWith(JUnit4.class)
public class VtoccTransactionHandlerTest {

  @Mock
  SqlQuery.BlockingInterface sqlQueryStubMock;

  @Mock
  Provider<RpcController> rpcControllerProvider;

  VtoccTransactionHandler vtoccTransactionHandler;
  SessionInfo sessionInfo = SessionInfo.newBuilder().setSessionId(42).build();

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
    vtoccTransactionHandler = new VtoccTransactionHandler(sqlQueryStubMock, "",
        rpcControllerProvider);
    when(sqlQueryStubMock.getSessionId(any(RpcController.class), any(SessionParams.class)))
        .thenReturn(sessionInfo).thenThrow(new IllegalStateException("Don't get session twice"));
    when(sqlQueryStubMock.begin(any(RpcController.class), any(Session.class)))
        .thenReturn(TransactionInfo.getDefaultInstance());
  }

  @Test
  public void testCommit() throws Exception {
    vtoccTransactionHandler.getSession();
    vtoccTransactionHandler.commit();

    verify(sqlQueryStubMock).getSessionId(any(RpcController.class), any(SessionParams.class));
    verify(sqlQueryStubMock).begin(any(RpcController.class), any(Session.class));
    verify(sqlQueryStubMock).commit(any(RpcController.class), any(Session.class));
    verifyNoMoreInteractions(sqlQueryStubMock);
  }

  @Test
  public void testBeginCommitBeginRollback() throws Exception {
    vtoccTransactionHandler.begin();
    vtoccTransactionHandler.commit();
    vtoccTransactionHandler.getSession();
    vtoccTransactionHandler.rollback();

    verify(sqlQueryStubMock).getSessionId(any(RpcController.class), any(SessionParams.class));
    verify(sqlQueryStubMock, times(2)).begin(any(RpcController.class), any(Session.class));
    verify(sqlQueryStubMock).commit(any(RpcController.class), any(Session.class));
    verify(sqlQueryStubMock).rollback(any(RpcController.class), any(Session.class));
    verifyNoMoreInteractions(sqlQueryStubMock);
  }

  @Test(expected = SQLException.class)
  public void testBeginException() throws Exception {
    when(sqlQueryStubMock.begin(any(RpcController.class), any(Session.class)))
        .thenThrow(new ServiceException(""));
    try {
      vtoccTransactionHandler.getSession();
      vtoccTransactionHandler.commit();
    } finally {
      verify(sqlQueryStubMock).getSessionId(any(RpcController.class), any(SessionParams.class));
      verify(sqlQueryStubMock).begin(any(RpcController.class), any(Session.class));
      verifyNoMoreInteractions(sqlQueryStubMock);
    }
  }

  @Test(expected = SQLException.class)
  public void testCommitException() throws Exception {
    when(sqlQueryStubMock.commit(any(RpcController.class), any(Session.class)))
        .thenThrow(new ServiceException(""));
    try {
      vtoccTransactionHandler.getSession();
      vtoccTransactionHandler.commit();
    } finally {
      verify(sqlQueryStubMock).getSessionId(any(RpcController.class), any(SessionParams.class));
      verify(sqlQueryStubMock).begin(any(RpcController.class), any(Session.class));
      verify(sqlQueryStubMock).commit(any(RpcController.class), any(Session.class));
      verifyNoMoreInteractions(sqlQueryStubMock);
    }
  }

  @Test(expected = SQLException.class)
  public void testRollbackException() throws Exception {
    when(sqlQueryStubMock.rollback(any(RpcController.class), any(Session.class)))
        .thenThrow(new ServiceException(""));
    try {
      vtoccTransactionHandler.getSession();
      vtoccTransactionHandler.rollback();
    } finally {
      verify(sqlQueryStubMock).getSessionId(any(RpcController.class), any(SessionParams.class));
      verify(sqlQueryStubMock).begin(any(RpcController.class), any(Session.class));
      verify(sqlQueryStubMock).rollback(any(RpcController.class), any(Session.class));
      verifyNoMoreInteractions(sqlQueryStubMock);
    }
  }
}
